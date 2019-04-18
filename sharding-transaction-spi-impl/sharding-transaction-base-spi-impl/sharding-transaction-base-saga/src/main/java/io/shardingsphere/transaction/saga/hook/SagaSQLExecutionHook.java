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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.SagaShardingTransactionManager;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaLogicSQLTransaction;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.resource.SagaTransactionResource;
import io.shardingsphere.transaction.saga.revert.RevertSQLEngineFactory;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.execute.hook.SQLExecutionHook;
import org.apache.shardingsphere.core.execute.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.core.metadata.datasource.DataSourceMetaData;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLUnit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Saga SQL execution hook.
 *
 * @author yangyi
 */
public final class SagaSQLExecutionHook implements SQLExecutionHook {
    
    private SagaTransaction globalTransaction;
    
    private SagaBranchTransaction branchTransaction;
    
    @Override
    public void start(final RouteUnit routeUnit, final DataSourceMetaData dataSourceMetaData, final boolean isTrunkThread, final Map<String, Object> shardingExecuteDataMap) {
        if (shardingExecuteDataMap.containsKey(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY)) {
            globalTransaction = (SagaTransaction) shardingExecuteDataMap.get(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY);
            if (!globalTransaction.getCurrentLogicSQLTransaction().isDMLLogicSQL()) {
                return;
            }
            branchTransaction = new SagaBranchTransaction(routeUnit.getDataSourceName(), routeUnit.getSqlUnit().getSql(), splitParameters(routeUnit.getSqlUnit()), ExecuteStatus.EXECUTING);
            globalTransaction.addBranchTransaction(branchTransaction);
            saveNewSnapshotIfNecessary(globalTransaction.getCurrentLogicSQLTransaction(), branchTransaction, routeUnit);
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
            ExecutorExceptionHandler.setExceptionThrown(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(globalTransaction.getRecoveryPolicy()));
            branchTransaction.setExecuteStatus(ExecuteStatus.FAILURE);
        }
    }
    
    private void saveNewSnapshotIfNecessary(final SagaLogicSQLTransaction logicSQLTransaction, final SagaBranchTransaction branchTransaction, final RouteUnit routeUnit) {
        if (RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(globalTransaction.getRecoveryPolicy())) {
            SagaTransactionResource transactionResource = SagaResourceManager.getTransactionResource(globalTransaction);
            Optional<RevertSQLUnit> revertSQLUnit = executeRevertSQL(logicSQLTransaction, routeUnit, transactionResource.getConnectionMap());
            this.branchTransaction.setRevertSQLUnit(revertSQLUnit.orNull());
            transactionResource.getPersistence().persistSnapshot(new SagaSnapshot(globalTransaction.getId(), branchTransaction.hashCode(), branchTransaction, revertSQLUnit.orNull()));
        }
    }
    
    private Optional<RevertSQLUnit> executeRevertSQL(final SagaLogicSQLTransaction logicSQLTransaction, final RouteUnit routeUnit, final ConcurrentMap<String, Connection> connectionMap) {
        try {
            return RevertSQLEngineFactory.newInstance(logicSQLTransaction.getSqlRouteResult(), routeUnit, logicSQLTransaction.getTableMetaData(), connectionMap).execute();
        } catch (final SQLException ex) {
            throw new ShardingException(String.format("Revert SQL %s failed: ", branchTransaction.toString()), ex);
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
        while (-1 != (currentIndex = sql.indexOf("?", currentIndex))) {
            result++;
            currentIndex += 1;
        }
        return result;
    }
    
}
