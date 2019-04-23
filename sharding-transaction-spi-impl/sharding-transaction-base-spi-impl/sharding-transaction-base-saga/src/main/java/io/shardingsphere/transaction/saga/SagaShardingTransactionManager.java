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
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaLogicSQLTransaction;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.revert.RevertSQLResult;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
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
 */
public final class SagaShardingTransactionManager implements ShardingTransactionManager {
    
    public static final String CURRENT_TRANSACTION_KEY = "current_transaction";
    
    private static final ThreadLocal<SagaTransaction> CURRENT_TRANSACTION = new ThreadLocal<>();
    
    private final Map<String, DataSource> dataSourceMap = new HashMap<>();
    
    /**
     * Get saga transaction for current thread.
     *
     * @return saga transaction
     */
    public static SagaTransaction getCurrentTransaction() {
        return CURRENT_TRANSACTION.get();
    }
    
    @Override
    public void init(final DatabaseType databaseType, final Collection<ResourceDataSource> resourceDataSources) {
        for (ResourceDataSource each : resourceDataSources) {
            registerDataSourceMap(each.getOriginalName(), each.getDataSource());
        }
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
    
    @Override
    public TransactionType getTransactionType() {
        return TransactionType.BASE;
    }
    
    @Override
    public boolean isInTransaction() {
        return null != CURRENT_TRANSACTION.get();
    }
    
    @Override
    public Connection getConnection(final String dataSourceName) throws SQLException {
        Connection result = dataSourceMap.get(dataSourceName).getConnection();
        CURRENT_TRANSACTION.get().getCachedConnections().put(dataSourceName, result);
        return result;
    }
    
    @Override
    public void begin() {
        if (null == CURRENT_TRANSACTION.get()) {
            SagaTransaction transaction = new SagaTransaction();
            ShardingExecuteDataMap.getDataMap().put(CURRENT_TRANSACTION_KEY, transaction);
            CURRENT_TRANSACTION.set(transaction);
        }
    }
    
    @Override
    @SneakyThrows
    public void commit() {
        if (null != CURRENT_TRANSACTION.get() && CURRENT_TRANSACTION.get().isContainsException()) {
            CURRENT_TRANSACTION.get().setTransactionOperationType(TransactionOperationType.COMMIT);
            SagaResourceManager.getInstance().getSagaExecutionComponent().run(getSagaDefinitionBuilder(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY).build());
        }
        cleanTransaction();
    }
    
    @Override
    @SneakyThrows
    public void rollback() {
        if (null != CURRENT_TRANSACTION.get()) {
            SagaDefinitionBuilder graphTaskBuilder = getSagaDefinitionBuilder(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY);
            graphTaskBuilder.addRollbackRequest();
            CURRENT_TRANSACTION.get().setTransactionOperationType(TransactionOperationType.ROLLBACK);
            SagaResourceManager.getInstance().getSagaExecutionComponent().run(graphTaskBuilder.build());
        }
        cleanTransaction();
    }
    
    private SagaDefinitionBuilder getSagaDefinitionBuilder(final String recoveryPolicy) {
        SagaConfiguration sagaConfiguration = SagaResourceManager.getInstance().getSagaConfiguration();
        SagaDefinitionBuilder result = new SagaDefinitionBuilder(recoveryPolicy, sagaConfiguration.getTransactionMaxRetries(),
            sagaConfiguration.getCompensationMaxRetries(), sagaConfiguration.getTransactionRetryDelayMilliseconds());
        for (SagaLogicSQLTransaction each : CURRENT_TRANSACTION.get().getLogicSQLTransactions()) {
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
    
    private void cleanTransaction() {
        if (null != CURRENT_TRANSACTION.get()) {
            SagaResourceManager.getInstance().getSagaPersistence().cleanSnapshot(CURRENT_TRANSACTION.get().getId());
        }
        ShardingExecuteDataMap.getDataMap().remove(CURRENT_TRANSACTION_KEY);
        CURRENT_TRANSACTION.remove();
    }
    
    @Override
    public void close() {
        dataSourceMap.clear();
    }
}
