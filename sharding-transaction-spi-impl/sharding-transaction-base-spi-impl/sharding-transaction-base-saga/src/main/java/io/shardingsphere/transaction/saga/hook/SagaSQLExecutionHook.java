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

package io.shardingsphere.transaction.saga.hook;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.SagaShardingTransactionManager;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.resource.SagaTransactionResource;
import io.shardingsphere.transaction.saga.revert.SQLRevertEngine;
import io.shardingsphere.transaction.saga.revert.SQLRevertResult;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.executor.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.core.metadata.datasource.DataSourceMetaData;
import org.apache.shardingsphere.core.routing.RouteUnit;
import org.apache.shardingsphere.core.routing.SQLUnit;
import org.apache.shardingsphere.core.routing.type.TableUnit;
import org.apache.shardingsphere.spi.hook.SQLExecutionHook;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Saga SQL execution hook.
 *
 * @author yangyi
 */
public final class SagaSQLExecutionHook implements SQLExecutionHook {
    
    private static final String SQL_PLACE_HOLDER = "?";
    
    private SagaTransaction sagaTransaction;
    
    private SagaBranchTransaction sagaBranchTransaction;
    
    @Override
    public void start(final RouteUnit routeUnit, final DataSourceMetaData dataSourceMetaData, final boolean isTrunkThread, final Map<String, Object> shardingExecuteDataMap) {
        if (shardingExecuteDataMap.containsKey(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY)) {
            sagaTransaction = (SagaTransaction) shardingExecuteDataMap.get(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY);
            if (sagaTransaction.isDMLBranchTransactionGroup()) {
                sagaBranchTransaction = new SagaBranchTransaction(routeUnit.getDataSourceName(), routeUnit.getSqlUnit().getSql(), splitParameters(routeUnit.getSqlUnit()));
                sagaBranchTransaction.setActualTableName(getAcutalTableName(routeUnit.getSqlUnit()));
                sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.EXECUTING);
                sagaTransaction.addBranchTransactionToGroup(sagaBranchTransaction);
                saveNewSnapshot();
            }
        }
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
        while (-1 != (currentIndex = sql.indexOf(SQL_PLACE_HOLDER, currentIndex))) {
            result++;
            currentIndex += 1;
        }
        return result;
    }
    
    private void saveNewSnapshot() {
        if (RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(sagaTransaction.getRecoveryPolicy())) {
            SagaTransactionResource transactionResource = SagaResourceManager.getTransactionResource(sagaTransaction);
            SQLRevertResult revertResult = sqlRevert(transactionResource.getRevertEngine());
            sagaTransaction.getRevertResults().put(sagaBranchTransaction, revertResult);
            transactionResource.getPersistence().persistSnapshot(new SagaSnapshot(sagaTransaction.getId(), sagaBranchTransaction.hashCode(), sagaBranchTransaction, revertResult));
        }
    }
    
    private SQLRevertResult sqlRevert(final SQLRevertEngine revertEngine) {
        try {
            return revertEngine.revert(sagaBranchTransaction, sagaTransaction.getCurrentBranchTransactionGroup());
        } catch (final SQLException ex) {
            throw new ShardingException(String.format("Revert SQL %s failed: ", sagaBranchTransaction.toString()), ex);
        }
    }
    
    private String getAcutalTableName(final SQLUnit sqlUnit) {
        Map<SQLUnit, TableUnit> tableUnitMap = sagaTransaction.getTableUnitMap();
        return tableUnitMap.containsKey(sqlUnit)
            ? tableUnitMap.get(sqlUnit).getRoutingTables().get(0).getActualTableName() : sagaTransaction.getCurrentBranchTransactionGroup().getSqlStatement().getTables().getSingleTableName();
    }
    
    @Override
    public void finishSuccess() {
        if (null != sagaTransaction && null != sagaBranchTransaction) {
            sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.SUCCESS);
        }
    }
    
    @Override
    public void finishFailure(final Exception cause) {
        if (null != sagaTransaction && null != sagaBranchTransaction) {
            ExecutorExceptionHandler.setExceptionThrown(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(sagaTransaction.getRecoveryPolicy()));
            sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.FAILURE);
        }
    }
}
