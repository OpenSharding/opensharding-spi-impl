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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.constant.SQLType;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parsing.parser.sql.SQLStatement;
import org.apache.shardingsphere.core.routing.SQLUnit;
import org.apache.shardingsphere.core.routing.type.TableUnit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
    
    private final Map<SQLUnit, TableUnit> tableUnitMap = new ConcurrentHashMap<>();
    
    private final Map<SagaBranchTransaction, ExecuteStatus> executionResults = new ConcurrentHashMap<>();
    
    private final Map<SagaBranchTransaction, SQLRevertResult> revertResults = new ConcurrentHashMap<>();
    
    private final List<SagaBranchTransactionGroup> branchTransactionGroups = new LinkedList<>();
    
    private SagaBranchTransactionGroup currentBranchTransactionGroup;
    
    private volatile boolean containsException;
    
    /**
     * Go to next branch transaction group.
     *
     * @param logicSQL logic sql
     * @param sqlStatement sql statement
     * @param shardingTableMetaData sharding table meta data
     */
    public void nextBranchTransactionGroup(final String logicSQL, final SQLStatement sqlStatement, final ShardingTableMetaData shardingTableMetaData) {
        currentBranchTransactionGroup = new SagaBranchTransactionGroup(logicSQL, sqlStatement, shardingTableMetaData);
        if (isDMLBranchTransactionGroup()) {
            branchTransactionGroups.add(currentBranchTransactionGroup);
        }
    }
    
    /**
     * Whether current branch transaction group is DML.
     *
     * @return current branch transaction group is DML
     */
    public boolean isDMLBranchTransactionGroup() {
        return SQLType.DML == currentBranchTransactionGroup.getSqlStatement().getType();
    }
    
    /**
     * Record execution result.
     *
     * @param sagaBranchTransaction saga branch transaction
     * @param executeStatus execute status
     */
    public void updateExecutionResult(final SagaBranchTransaction sagaBranchTransaction, final ExecuteStatus executeStatus) {
        containsException |= ExecuteStatus.FAILURE == executeStatus;
        executionResults.put(sagaBranchTransaction, executeStatus);
    }
    
    /**
     * Save new snapshot.
     *
     * @param sagaBranchTransaction saga branch transaction
     */
    public void saveNewSnapshot(final SagaBranchTransaction sagaBranchTransaction) {
        currentBranchTransactionGroup.getBranchTransactions().add(sagaBranchTransaction);
        if (RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(sagaConfiguration.getRecoveryPolicy())) {
            sqlRevert(sagaBranchTransaction);
            persistence.persistSnapshot(new SagaSnapshot(id, sagaBranchTransaction.hashCode(), sagaBranchTransaction, revertResults.get(sagaBranchTransaction)));
        }
    }
    
    private void sqlRevert(final SagaBranchTransaction sagaBranchTransaction) {
        SQLRevertEngine sqlRevertEngine = new SQLRevertEngine(connections);
        try {
            revertResults.put(sagaBranchTransaction, sqlRevertEngine.revert(sagaBranchTransaction, currentBranchTransactionGroup));
        } catch (final SQLException ex) {
            throw new ShardingException(String.format("Revert SQL %s failed: ", sagaBranchTransaction.toString()), ex);
        }
    }
    
    /**
     * Clean snapshot.
     */
    public void cleanSnapshot() {
        persistence.cleanSnapshot(id);
    }
}
