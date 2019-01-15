/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.saga;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.transaction.api.TransactionType;
import io.shardingsphere.transaction.saga.manager.SagaTransactionManager;
import io.shardingsphere.transaction.spi.ShardingTransactionEngine;

import javax.sql.DataSource;
import javax.transaction.Status;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Sharding transaction engine for Saga.
 *
 * @author yangyi
 */
public final class SagaShardingTransactionEngine implements ShardingTransactionEngine {
    
    private final SagaTransactionManager sagaTransactionManager = SagaTransactionManager.getInstance();
    
    @Override
    public void init(final DatabaseType databaseType, final Map<String, DataSource> dataSourceMap) {
        sagaTransactionManager.getResourceManager().registerDataSourceMap(dataSourceMap);
    }
    
    @Override
    public TransactionType getTransactionType() {
        return TransactionType.BASE;
    }
    
    @Override
    public boolean isInTransaction() {
        return Status.STATUS_NO_TRANSACTION != sagaTransactionManager.getStatus();
    }
    
    @Override
    public Connection getConnection(final String dataSourceName) throws SQLException {
        Connection result = sagaTransactionManager.getResourceManager().getConnection(dataSourceName);
        if (null != sagaTransactionManager.getTransaction() && !sagaTransactionManager.getTransaction().getConnectionMap().containsKey(dataSourceName)) {
            sagaTransactionManager.getTransaction().getConnectionMap().put(dataSourceName, result);
        }
        return result;
    }
    
    @Override
    public void begin() {
        sagaTransactionManager.begin();
    }
    
    @Override
    public void commit() {
        sagaTransactionManager.commit();
    }
    
    @Override
    public void rollback() {
        sagaTransactionManager.rollback();
    }
    
    @Override
    public void close() {
        sagaTransactionManager.getResourceManager().releaseDataSourceMap();
    }
}
