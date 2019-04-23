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

import io.shardingsphere.transaction.saga.core.SagaTransactionHolder;
import io.shardingsphere.transaction.saga.core.resource.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.core.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.core.context.SagaLogicSQLTransaction;
import io.shardingsphere.transaction.saga.core.context.SagaTransaction;
import io.shardingsphere.transaction.saga.core.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.core.revert.RevertSQLResult;
import io.shardingsphere.transaction.saga.core.resource.servicecomb.definition.SagaDefinitionBuilder;
import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.execute.ShardingExecuteDataMap;
import org.apache.shardingsphere.transaction.core.ResourceDataSource;
import org.apache.shardingsphere.transaction.core.TransactionOperationType;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.spi.ShardingTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Sharding transaction manager for Saga.
 *
 * @author yangyi
 * @author zhaojun
 */
public final class SagaShardingTransactionManager implements ShardingTransactionManager {
    
    public static final String SAGA_TRANSACTION_KEY = "saga_transaction";
    
    private final Map<String, DataSource> dataSourceMap = new HashMap<>();
    
    @Override
    public void init(final DatabaseType databaseType, final Collection<ResourceDataSource> resourceDataSources) {
        for (ResourceDataSource each : resourceDataSources) {
            registerDataSourceMap(each.getOriginalName(), each.getDataSource());
        }
    }
    
    @Override
    public TransactionType getTransactionType() {
        return TransactionType.BASE;
    }
    
    @Override
    public boolean isInTransaction() {
        return SagaTransactionHolder.isInTransaction();
    }
    
    @Override
    public Connection getConnection(final String dataSourceName) throws SQLException {
        Connection result = dataSourceMap.get(dataSourceName).getConnection();
        SagaTransactionHolder.get().getCachedConnections().put(dataSourceName, result);
        return result;
    }
    
    @Override
    public void begin() {
        if (!SagaTransactionHolder.isInTransaction()) {
            SagaTransaction sagaTransaction = new SagaTransaction();
            SagaTransactionHolder.set(sagaTransaction);
            ShardingExecuteDataMap.getDataMap().put(SAGA_TRANSACTION_KEY, sagaTransaction);
        }
    }
    
    @Override
    @SneakyThrows
    public void commit() {
        if (SagaTransactionHolder.isInTransaction() && SagaTransactionHolder.get().isContainsException()) {
            SagaTransactionHolder.get().setTransactionOperationType(TransactionOperationType.COMMIT);
            SagaResourceManager.getInstance().getSagaExecutionComponent().run(getSagaDefinitionBuilder(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY).build());
        }
        clearSagaTransaction();
    }
    
    @Override
    @SneakyThrows
    public void rollback() {
        if (SagaTransactionHolder.isInTransaction()) {
            SagaDefinitionBuilder builder = getSagaDefinitionBuilder(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY);
            builder.addRollbackRequest();
            SagaTransactionHolder.get().setTransactionOperationType(TransactionOperationType.ROLLBACK);
            SagaResourceManager.getInstance().getSagaExecutionComponent().run(builder.build());
        }
        clearSagaTransaction();
    }
    
    @Override
    public void close() {
        dataSourceMap.clear();
    }
    
    private void registerDataSourceMap(final String datasourceName, final DataSource dataSource) {
        validateDataSourceName(datasourceName);
        dataSourceMap.put(datasourceName, dataSource);
    }
    
    private void validateDataSourceName(final String datasourceName) {
        if (dataSourceMap.containsKey(datasourceName)) {
            throw new ShardingException("datasource {} has registered", datasourceName);
        }
    }
    
    private SagaDefinitionBuilder getSagaDefinitionBuilder(final String recoveryPolicy) {
        SagaConfiguration sagaConfiguration = SagaResourceManager.getInstance().getSagaConfiguration();
        SagaDefinitionBuilder result = new SagaDefinitionBuilder(recoveryPolicy, sagaConfiguration.getTransactionMaxRetries(),
            sagaConfiguration.getCompensationMaxRetries(), sagaConfiguration.getTransactionRetryDelayMilliseconds());
        for (SagaLogicSQLTransaction each : SagaTransactionHolder.get().getLogicSQLTransactions()) {
            result.nextLogicSQL();
            addLogicSQLDefinition(result, each);
        }
        return result;
    }
    
    private void addLogicSQLDefinition(final SagaDefinitionBuilder sagaDefinitionBuilder, final SagaLogicSQLTransaction sagaLogicSQLTransaction) {
        RevertSQLResult defaultValue = new RevertSQLResult("");
        for (SagaBranchTransaction each : sagaLogicSQLTransaction.getBranchTransactions()) {
            RevertSQLResult revertSQLUnit = null != each.getRevertSQLResult() ? each.getRevertSQLResult() : defaultValue;
            sagaDefinitionBuilder.addSagaRequest(each.getBranchId(), each.getDataSourceName(), each.getSql(), each.getParameters(), revertSQLUnit.getSql(), revertSQLUnit.getParameters());
        }
    }
    
    private void clearSagaTransaction() {
        if (SagaTransactionHolder.isInTransaction()) {
            SagaResourceManager.getInstance().getSagaPersistence().cleanSnapshot(SagaTransactionHolder.get().getId());
        }
        ShardingExecuteDataMap.getDataMap().remove(SAGA_TRANSACTION_KEY);
        SagaTransactionHolder.clear();
    }
}
