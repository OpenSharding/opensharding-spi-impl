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

package io.shardingsphere.transaction.saga.context;

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.revert.SQLRevertResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.constant.SQLType;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parsing.parser.sql.SQLStatement;
import org.apache.shardingsphere.core.routing.SQLUnit;
import org.apache.shardingsphere.core.routing.type.TableUnit;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Saga transaction.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Getter
public final class SagaTransaction {
    
    private final String id = UUID.randomUUID().toString();
    
    private final String recoveryPolicy;
    
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
     * Add new branch transaction to current transaction group.
     *
     * @param branchTransaction saga branch transaction
     */
    public void addBranchTransactionToGroup(final SagaBranchTransaction branchTransaction) {
        currentBranchTransactionGroup.getBranchTransactions().add(branchTransaction);
    }
}
