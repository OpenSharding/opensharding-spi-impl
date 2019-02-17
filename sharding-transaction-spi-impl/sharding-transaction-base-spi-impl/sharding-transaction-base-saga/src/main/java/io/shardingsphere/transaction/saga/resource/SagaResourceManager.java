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

package io.shardingsphere.transaction.saga.resource;

import io.shardingsphere.transaction.saga.SagaTransaction;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaPersistenceLoader;
import io.shardingsphere.transaction.saga.servicecomb.SagaExecutionComponentFactory;
import lombok.Getter;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;
import org.apache.shardingsphere.core.exception.ShardingException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Saga resource manager.
 *
 * @author yangyi
 */
public final class SagaResourceManager {
    
    private static final Map<SagaTransaction, SagaTransactionResource> TRANSACTION_RESOURCE_MAP = new ConcurrentHashMap<>();
    
    @Getter
    private final SagaPersistence sagaPersistence;
    
    @Getter
    private final SagaExecutionComponent sagaExecutionComponent;
    
    private final Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();
    
    public SagaResourceManager(final SagaConfiguration sagaConfiguration) {
        sagaPersistence = SagaPersistenceLoader.load(sagaConfiguration.getSagaPersistenceConfiguration());
        sagaExecutionComponent = SagaExecutionComponentFactory.createSagaExecutionComponent(sagaConfiguration, sagaPersistence);
    }
    
    /**
     * Get saga transaction resource.
     *
     * @param sagaTransaction saga transaction
     * @return saga transaction resource
     */
    public static SagaTransactionResource getTransactionResource(final SagaTransaction sagaTransaction) {
        return TRANSACTION_RESOURCE_MAP.get(sagaTransaction);
    }
    
    /**
     * Register transaction resource.
     *
     * @param sagaTransaction saga transaction
     */
    public void registerTransactionResource(final SagaTransaction sagaTransaction) {
        TRANSACTION_RESOURCE_MAP.put(sagaTransaction, new SagaTransactionResource(sagaPersistence));
    }
    
    /**
     * Release transaction resource.
     *
     * @param sagaTransaction saga transaction
     */
    public void releaseTransactionResource(final SagaTransaction sagaTransaction) {
        TRANSACTION_RESOURCE_MAP.remove(sagaTransaction);
    }
    
    /**
     * Register data source map.
     *
     * @param datasourceName data sourceName
     * @param dataSource data source
     */
    public void registerDataSourceMap(final String datasourceName, final DataSource dataSource) {
        validateDataSourceName(datasourceName, dataSource);
        dataSourceMap.put(datasourceName, dataSource);
    }
    
    private void validateDataSourceName(final String datasourceName, final DataSource dataSource) {
        if (dataSourceMap.containsKey(datasourceName)) {
            throw new ShardingException("datasource {} has registered", datasourceName);
        }
    }
    
    /**
     * Get connection.
     * 
     * @param dataSourceName data source name
     * @param sagaTransaction saga transaction
     * @return connection
     * @throws SQLException SQL exception
     */
    public Connection getConnection(final String dataSourceName, final SagaTransaction sagaTransaction) throws SQLException {
        Connection result = dataSourceMap.get(dataSourceName).getConnection();
        TRANSACTION_RESOURCE_MAP.get(sagaTransaction).getConnections().putIfAbsent(dataSourceName, result);
        return result;
    }
    
    /**
     * Release data source map.
     */
    public void releaseDataSourceMap() {
        dataSourceMap.clear();
    }
}
