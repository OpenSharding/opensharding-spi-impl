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

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.route.SQLRouteResult;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

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
    
    private final List<SagaLogicSQLTransaction> logicSQLTransactions = new LinkedList<>();
    
    private SagaLogicSQLTransaction currentLogicSQLTransaction;
    
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
    
    /**
     * Add new branch transaction to current logic SQL transaction.
     *
     * @param branchTransaction saga branch transaction
     */
    public void addBranchTransaction(final SagaBranchTransaction branchTransaction) {
        currentLogicSQLTransaction.getBranchTransactions().add(branchTransaction);
    }
    
    /**
     * Change all logic transaction status.
     *
     * @param executeStatus execute status
     */
    public void changeAllLogicTransactionStatus(final ExecuteStatus executeStatus) {
        for (SagaLogicSQLTransaction each : logicSQLTransactions) {
            changeAllBranchTransactionStatus(each, executeStatus);
        }
    }
    
    private void changeAllBranchTransactionStatus(final SagaLogicSQLTransaction logicSQLTransaction, final ExecuteStatus executeStatus) {
        for (SagaBranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
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
    public Optional<SagaBranchTransaction> findBranchTransaction(final String dataSourceName, final String sql, final List<List<String>> sagaParameters) {
        Optional<SagaBranchTransaction> result = Optional.absent();
        for (SagaLogicSQLTransaction each : logicSQLTransactions) {
            result = doFindBranchTransaction(each, dataSourceName, sql, sagaParameters);
            if (result.isPresent()) {
                return result;
            }
        }
        return result;
    }
    
    private Optional<SagaBranchTransaction> doFindBranchTransaction(final SagaLogicSQLTransaction logicSQLTransaction, final String dataSourceName,
                                                                    final String sql, final List<List<String>> sagaParameters) {
        for (SagaBranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
            if (dataSourceName.equals(each.getDataSourceName())) {
                if (ExecuteStatus.COMPENSATING.equals(each.getExecuteStatus()) && sql.equals(each.getRevertSQLResult().getSql())
                    && judgeRevertParameters(sagaParameters, each.getRevertSQLResult().getParameters())) {
                    return Optional.of(each);
                } else if (!ExecuteStatus.COMPENSATING.equals(each.getExecuteStatus()) && sql.equals(each.getSql())
                    && judgeParameters(sagaParameters, each.getParameterSets())) {
                    return Optional.of(each);
                }
            }
        }
        return Optional.absent();
    }
    
    private boolean judgeParameters(List<List<String>> sagaParameters, List<List<Object>> sqlParameters) {
        Iterator<List<String>> sagaParameterIterator = sagaParameters.iterator();
        Iterator<List<Object>> sqlParameterIterator = sqlParameters.iterator();
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
