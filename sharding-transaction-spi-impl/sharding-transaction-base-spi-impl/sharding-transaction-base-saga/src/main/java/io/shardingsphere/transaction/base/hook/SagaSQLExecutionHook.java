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

package io.shardingsphere.transaction.base.hook;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.base.context.ExecuteStatus;
import io.shardingsphere.transaction.base.context.SagaBranchTransaction;
import io.shardingsphere.transaction.base.context.SagaLogicSQLTransaction;
import io.shardingsphere.transaction.base.context.SagaTransaction;
import io.shardingsphere.transaction.base.saga.SagaShardingTransactionManager;
import io.shardingsphere.transaction.base.hook.revert.DMLSQLRevertEngine;
import io.shardingsphere.transaction.base.hook.revert.RevertSQLResult;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutorFactory;
import org.apache.shardingsphere.core.execute.hook.SQLExecutionHook;
import org.apache.shardingsphere.core.metadata.datasource.DataSourceMetaData;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLUnit;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Saga SQL execution hook.
 *
 * @author yangyi
 * @author zhaojun
 */
public final class SagaSQLExecutionHook implements SQLExecutionHook {
    
    private SagaTransaction sagaTransaction;
    
    private SagaBranchTransaction branchTransaction;
    
    @Override
    public void start(final RouteUnit routeUnit, final DataSourceMetaData dataSourceMetaData, final boolean isTrunkThread, final Map<String, Object> shardingExecuteDataMap) {
        if (!shardingExecuteDataMap.containsKey(SagaShardingTransactionManager.SAGA_TRANSACTION_KEY)) {
            return;
        }
        sagaTransaction = (SagaTransaction) shardingExecuteDataMap.get(SagaShardingTransactionManager.SAGA_TRANSACTION_KEY);
        if (sagaTransaction.getCurrentLogicSQLTransaction().isDMLLogicSQL()) {
            branchTransaction = new SagaBranchTransaction(routeUnit.getDataSourceName(), routeUnit.getSqlUnit().getSql(), splitParameters(routeUnit.getSqlUnit()), ExecuteStatus.EXECUTING);
            branchTransaction.setRevertSQLResult(doSQLRevert(sagaTransaction.getCurrentLogicSQLTransaction(), routeUnit).orNull());
            sagaTransaction.addBranchTransaction(branchTransaction);
        }
    }
    
    @Override
    public void finishSuccess() {
        if (null != branchTransaction) {
            branchTransaction.setExecuteStatus(ExecuteStatus.SUCCESS);
        }
    }
    
    @Override
    public void finishFailure(final Exception cause) {
        if (null != branchTransaction) {
            branchTransaction.setExecuteStatus(ExecuteStatus.FAILURE);
        }
    }
    
    private Optional<RevertSQLResult> doSQLRevert(final SagaLogicSQLTransaction logicSQLTransaction, final RouteUnit routeUnit) {
        Connection connection = sagaTransaction.getCachedConnections().get(routeUnit.getDataSourceName());
        SQLRevertExecutorContext context = new SQLRevertExecutorContext(logicSQLTransaction.getSqlRouteResult(), routeUnit, logicSQLTransaction.getTableMetaData(), connection);
        return new DMLSQLRevertEngine(SQLRevertExecutorFactory.newInstance(context)).revert();
    }
    
    private List<List<Object>> splitParameters(final SQLUnit sqlUnit) {
        List<List<Object>> result = Lists.newArrayList();
        int placeholderCount = countPlaceholder(sqlUnit.getSql());
        if (placeholderCount == sqlUnit.getParameters().size()) {
            result.add(sqlUnit.getParameters());
        } else {
            result.addAll(Lists.partition(sqlUnit.getParameters(), placeholderCount));
        }
        return result;
    }
    
    private int countPlaceholder(final String sql) {
        int result = 0;
        int currentIndex = 0;
        while (-1 != (currentIndex = sql.indexOf("?", currentIndex))) {
            result++;
            currentIndex += 1;
        }
        return result;
    }
    
}
