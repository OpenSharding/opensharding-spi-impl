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

package io.shardingsphere.transaction.base;

import io.shardingsphere.transaction.base.context.ShardingSQLTransaction;
import io.shardingsphere.transaction.base.saga.ShardingSQLTransactionManager;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.spi.database.DatabaseType;
import org.apache.shardingsphere.transaction.core.ResourceDataSource;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.spi.ShardingTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Saga Sharding transaction manager.
 *
 * @author yangyi
 * @author zhaojun
 */
public final class SagaShardingTransactionManager implements ShardingTransactionManager {
    
    private final Map<String, DataSource> dataSourceMap = new HashMap<>();
    
    private final ShardingSQLTransactionManager shardingSQLTransactionManager = ShardingSQLTransactionManager.getInstance();
    
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
        return shardingSQLTransactionManager.isInTransaction();
    }
    
    /**
     * Get current transaction.
     *
     * @return current transaction
     */
    public ShardingSQLTransaction getCurrentTransaction() {
        return shardingSQLTransactionManager.getCurrentTransaction();
    }
    
    @Override
    public Connection getConnection(final String dataSourceName) throws SQLException {
        Connection result = dataSourceMap.get(dataSourceName).getConnection();
        if (isInTransaction()) {
            shardingSQLTransactionManager.getCurrentTransaction().getCachedConnections().put(dataSourceName, result);
        }
        return result;
    }
    
    @Override
    public void begin() {
        shardingSQLTransactionManager.begin();
    }
    
    @Override
    public void commit() {
        shardingSQLTransactionManager.commit();
    }
    
    @Override
    public void rollback() {
        shardingSQLTransactionManager.rollback();
    }
    
    @Override
    public void close() {
        dataSourceMap.clear();
    }
}
