/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.shardingsphere.transaction.saga;

import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.revert.SQLRevertEngine;
import io.shardingsphere.transaction.saga.revert.SQLRevertResult;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.exception.ShardingException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Saga transaction.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Getter
public final class SagaTransaction {
    
    private final String id = UUID.randomUUID().toString();
    
    private final SagaConfiguration sagaConfiguration;
    
    private final SagaPersistence persistence;
    
    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();
    
    private final Map<SagaBranchTransaction, ExecuteStatus> executionResults = new ConcurrentHashMap<>();
    
    private final Map<SagaBranchTransaction, SQLRevertResult> revertResults = new ConcurrentHashMap<>();
    
    private final List<Queue<SagaBranchTransaction>> branchTransactionGroups = new LinkedList<>();
    
    private Queue<SagaBranchTransaction> currentBranchTransactionGroup;
    
    private volatile boolean containsException;
    
    /**
     * Go to next branch transaction group.
     */
    public void nextBranchTransactionGroup() {
        currentBranchTransactionGroup = new ConcurrentLinkedQueue<>();
        branchTransactionGroups.add(currentBranchTransactionGroup);
    }
    
    /**
     * Record execution result.
     *
     * @param sagaBranchTransaction saga branch transaction
     * @param executeStatus execute status
     */
    public void updateExecutionResult(final SagaBranchTransaction sagaBranchTransaction, final ExecuteStatus executeStatus) {
        containsException = ExecuteStatus.FAILURE == executeStatus;
        executionResults.put(sagaBranchTransaction, executeStatus);
    }
    
    /**
     * Save new snapshot.
     *
     * @param sagaBranchTransaction saga branch transaction
     */
    public void saveNewSnapshot(final SagaBranchTransaction sagaBranchTransaction) {
        currentBranchTransactionGroup.add(sagaBranchTransaction);
        if (RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(sagaConfiguration.getRecoveryPolicy())) {
            sqlRevert(sagaBranchTransaction);
        }
        persistence.persistSnapshot(new SagaSnapshot(id, sagaBranchTransaction.hashCode(), sagaBranchTransaction, revertResults.get(sagaBranchTransaction), ExecuteStatus.EXECUTING));
    }
    
    private void sqlRevert(final SagaBranchTransaction sagaBranchTransaction) {
        SQLRevertEngine sqlRevertEngine = new SQLRevertEngine(connections);
        try {
            revertResults.put(sagaBranchTransaction, sqlRevertEngine.revert(sagaBranchTransaction.getDataSourceName(), sagaBranchTransaction.getSql(), sagaBranchTransaction.getParameterSets()));
        } catch (final SQLException ex) {
            throw new ShardingException(String.format("Revert SQL %s failed: ", sagaBranchTransaction.toString()), ex);
        }
    }
    
    /**
     * Update snapshot.
     *
     * @param sagaBranchTransaction saga branch transaction
     * @param executeStatus execute status
     */
    public void updateSnapshot(final SagaBranchTransaction sagaBranchTransaction, final ExecuteStatus executeStatus) {
        persistence.updateSnapshotStatus(id, sagaBranchTransaction.hashCode(), executeStatus);
    }
    
    /**
     * Get saga definition builder.
     *
     * @return saga definition builder
     */
    public SagaDefinitionBuilder getSagaDefinitionBuilder() {
        SagaDefinitionBuilder result = new SagaDefinitionBuilder(sagaConfiguration.getRecoveryPolicy(), 
                sagaConfiguration.getTransactionMaxRetries(), sagaConfiguration.getCompensationMaxRetries(), sagaConfiguration.getTransactionRetryDelayMilliseconds());
        for (Queue<SagaBranchTransaction> each : branchTransactionGroups) {
            result.switchParents();
            initSagaDefinitionForGroup(result, each);
        }
        return result;
    }
    
    private void initSagaDefinitionForGroup(final SagaDefinitionBuilder sagaDefinitionBuilder, final Queue<SagaBranchTransaction> sagaBranchTransactionGroup) {
        for (SagaBranchTransaction each : sagaBranchTransactionGroup) {
            SQLRevertResult revertResult = revertResults.containsKey(each) ? revertResults.get(each) : new SQLRevertResult();
            sagaDefinitionBuilder.addChildRequest(
                    String.valueOf(each.hashCode()), each.getDataSourceName(), each.getSql(), each.getParameterSets(), revertResult.getSql(), revertResult.getParameterSets());
        }
    }
    
    /**
     * Clean snapshot in persistence.
     */
    public void cleanSnapshot() {
        persistence.cleanSnapshot(id);
    }
}
