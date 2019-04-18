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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.SQLUnit;
import org.apache.shardingsphere.core.route.type.TableUnit;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Saga transaction.
 *
 * @author yangyi
 * @author zhaojun
 */
@RequiredArgsConstructor
@Getter
public final class SagaTransaction {
    
    private final String id = UUID.randomUUID().toString();
    
    private final String recoveryPolicy;
    
    private final Map<SQLUnit, TableUnit> tableUnitMap = new ConcurrentHashMap<>();
    
    private final List<SagaLogicSQLTransaction> logicSQLTransactions = new LinkedList<>();
    
    private SagaLogicSQLTransaction currentLogicSQLTransaction;
    
//    private volatile boolean containsException;
    
    /**
     * Go to next logic SQL transaction.
     *
     * @param logicSQL logic SQL
     * @param sqlRouteResult SQL route result
     * @param shardingTableMetaData sharding table meta data
     */
    public void nextLogicSQLTransaction(final String logicSQL, final SQLRouteResult sqlRouteResult, final ShardingTableMetaData shardingTableMetaData) {
        currentLogicSQLTransaction = new SagaLogicSQLTransaction(logicSQL, sqlRouteResult, shardingTableMetaData);
        if (currentLogicSQLTransaction.isDMLLogicSQL()) {
            logicSQLTransactions.add(currentLogicSQLTransaction);
        }
    }
    
//    /**
//     * Whether current logic SQL transaction is DML.
//     *
//     * @return current branch transaction group is DML
//     */
//    public boolean isDMLLogicSQLTransaction() {
//        return SQLType.DML == currentLogicSQLTransaction.getSqlStatement().getType();
//    }
    
    /**
     * Whether saga transaction contains exception or not.
     *
     * @return true or false
     */
    public boolean isContainsException() {
        for (SagaLogicSQLTransaction each : logicSQLTransactions) {
            if (isLogicSQLTransactionFailed(each)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isLogicSQLTransactionFailed(final SagaLogicSQLTransaction logicSQLTransaction) {
        for (SagaBranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
            if (ExecuteStatus.FAILURE.equals(each.getExecuteStatus())) {
                return true;
            }
        }
        return false;
    }
    
//    /**
//     * Record execution result.
//     *
//     * @param sagaBranchTransaction saga branch transaction
//     * @param executeStatus execute status
//     */
//    public void updateExecutionResult(final SagaBranchTransaction sagaBranchTransaction, final ExecuteStatus executeStatus) {
//        if (containsException | ExecuteStatus.FAILURE == executeStatus) {
//            containsException = true;
//        }
//        executionResults.put(sagaBranchTransaction, executeStatus);
//    }
    
    /**
     * Add new branch transaction to current logic SQL transaction.
     *
     * @param branchTransaction saga branch transaction
     */
    public void addBranchTransaction(final SagaBranchTransaction branchTransaction) {
        currentLogicSQLTransaction.getBranchTransactions().add(branchTransaction);
    }
}
