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

package io.shardingsphere.transaction.base.context;

import com.google.common.base.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.transaction.core.TransactionOperationType;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Transaction context.
 *
 * @author yangyi
 * @author zhaojun
 */
@RequiredArgsConstructor
@Getter
public final class TransactionContext {
    
    private final String id = UUID.randomUUID().toString();
    
    private final List<LogicSQLTransaction> logicSQLTransactions = new LinkedList<>();
    
    private LogicSQLTransaction currentLogicSQLTransaction;
    
    private final Map<String, Connection> cachedConnections = new HashMap<>();
    
    @Setter
    private TransactionOperationType operationType = TransactionOperationType.BEGIN;
    
    /**
     * Go to next logic SQL transaction.
     *
     * @param sql logic SQL
     */
    public void nextLogicSQLTransaction(final String sql) {
        currentLogicSQLTransaction = new LogicSQLTransaction(sql);
    }
    
    /**
     * Go to next logic SQL transaction.
     *
     * @param sqlRouteResult SQL route result
     * @param shardingTableMetaData sharding table meta data
     */
    public void initLogicSQLTransaction(final SQLRouteResult sqlRouteResult, final ShardingTableMetaData shardingTableMetaData) {
        currentLogicSQLTransaction.doInit(sqlRouteResult, shardingTableMetaData);
        if (currentLogicSQLTransaction.isWritableTransaction()) {
            logicSQLTransactions.add(currentLogicSQLTransaction);
        }
    }
    
    /**
     * Whether branch transactions contain exception or not.
     *
     * @return true or false
     */
    public boolean isContainsException() {
        for (LogicSQLTransaction each : logicSQLTransactions) {
            if (isLogicSQLTransactionFailed(each)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isLogicSQLTransactionFailed(final LogicSQLTransaction logicSQLTransaction) {
        for (BranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
            if (ExecuteStatus.FAILURE.equals(each.getExecuteStatus())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Add new branch transaction to current logic SQL transaction.
     *
     * @param branchTransaction branch transaction
     */
    public void addBranchTransaction(final BranchTransaction branchTransaction) {
        currentLogicSQLTransaction.getBranchTransactions().add(branchTransaction);
    }
    
    /**
     * Change all logic transactions status.
     *
     * @param executeStatus execute status
     */
    public void changeAllLogicTransactionStatus(final ExecuteStatus executeStatus) {
        for (LogicSQLTransaction each : logicSQLTransactions) {
            changeAllBranchTransactionStatus(each, executeStatus);
        }
    }
    
    private void changeAllBranchTransactionStatus(final LogicSQLTransaction logicSQLTransaction, final ExecuteStatus executeStatus) {
        for (BranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
            each.setExecuteStatus(executeStatus);
        }
    }
    
    /**
     * Find branch transaction.
     *
     * @param dataSourceName data source name
     * @param sql SQL
     * @param sagaParameters saga parameters
     * @return saga branch transaction
     */
    public Optional<BranchTransaction> findBranchTransaction(final String dataSourceName, final String sql, final List<List<String>> sagaParameters) {
        Optional<BranchTransaction> result = Optional.absent();
        for (LogicSQLTransaction each : logicSQLTransactions) {
            result = doFindBranchTransaction(each, dataSourceName, sql, sagaParameters);
            if (result.isPresent()) {
                return result;
            }
        }
        return result;
    }
    
    private Optional<BranchTransaction> doFindBranchTransaction(final LogicSQLTransaction logicSQLTransaction, final String dataSourceName,
                                                                final String sql, final List<List<String>> sagaParameters) {
        for (BranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
            if (dataSourceName.equals(each.getDataSourceName())) {
                if (ExecuteStatus.COMPENSATING.equals(each.getExecuteStatus()) && sql.equals(each.getRevertSQLResult().getSql())
                    && judgeRevertParameters(sagaParameters, each.getRevertSQLResult().getParameters())) {
                    return Optional.of(each);
                } else if (!ExecuteStatus.COMPENSATING.equals(each.getExecuteStatus()) && sql.equals(each.getSql())
                    && judgeParameters(sagaParameters, each.getParameters())) {
                    return Optional.of(each);
                }
            }
        }
        return Optional.absent();
    }
    
    private boolean judgeParameters(List<List<String>> sagaParameters, List<Collection<Object>> sqlParameters) {
        Iterator<List<String>> sagaParameterIterator = sagaParameters.iterator();
        Iterator<Collection<Object>> sqlParameterIterator = sqlParameters.iterator();
        while (sagaParameterIterator.hasNext()) {
            if (!sagaParameterIterator.next().toString().equals(sqlParameterIterator.next().toString())) {
                return false;
            }
        }
        return true;
    }
    
    private boolean judgeRevertParameters(List<List<String>> sagaParameters, List<Collection<Object>> revertParameters) {
        Iterator<List<String>> sagaParameterIterator = sagaParameters.iterator();
        Iterator<Collection<Object>> revertParameterIterator = revertParameters.iterator();
        while (sagaParameterIterator.hasNext()) {
            if (!sagaParameterIterator.next().toString().equals(revertParameterIterator.next().toString())) {
                return false;
            }
        }
        return true;
    }
}
