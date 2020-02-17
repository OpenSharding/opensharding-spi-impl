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

package io.opensharding.transaction.base.saga.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.opensharding.transaction.base.saga.config.SagaPersistenceConfiguration;
import io.opensharding.transaction.base.saga.persistence.impl.EmptySagaPersistence;
import io.opensharding.transaction.base.saga.persistence.impl.jdbc.JDBCSagaPersistence;
import org.apache.servicecomb.saga.core.PersistentStore;

import javax.sql.DataSource;
import java.util.ServiceLoader;

/**
 * Saga persistence loader.
 *
 * @author yangyi
 */
public final class SagaPersistenceLoader {
    
    /**
     * Load saga persistence.
     *
     * @param persistenceConfiguration persistence configuration
     * @return saga persistence
     */
    public static PersistentStore load(final SagaPersistenceConfiguration persistenceConfiguration) {
        if (!persistenceConfiguration.isEnablePersistence()) {
            return new EmptySagaPersistence();
        }
        PersistentStore result = null;
        for (PersistentStore each : ServiceLoader.load(PersistentStore.class)) {
            result = each;
        }
        if (null == result) {
            result = loadDefaultPersistence(persistenceConfiguration);
        }
        return result;
    }
    
    private static PersistentStore loadDefaultPersistence(final SagaPersistenceConfiguration persistenceConfiguration) {
        JDBCSagaPersistence result = new JDBCSagaPersistence(initDataSource(persistenceConfiguration));
        result.createTableIfNotExists();
        return result;
    }
    
    private static DataSource initDataSource(final SagaPersistenceConfiguration persistenceConfiguration) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(persistenceConfiguration.getUrl());
        config.setUsername(persistenceConfiguration.getUsername());
        config.setPassword(persistenceConfiguration.getPassword());
        config.setConnectionTimeout(persistenceConfiguration.getConnectionTimeoutMilliseconds());
        config.setIdleTimeout(persistenceConfiguration.getIdleTimeoutMilliseconds());
        config.setMaxLifetime(persistenceConfiguration.getMaxLifetimeMilliseconds());
        config.setMaximumPoolSize(persistenceConfiguration.getMaxPoolSize());
        config.setMinimumIdle(persistenceConfiguration.getMinPoolSize());
        config.addDataSourceProperty("useServerPrepStmts", Boolean.TRUE.toString());
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", 250);
        config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        config.addDataSourceProperty("useLocalSessionState", Boolean.TRUE.toString());
        config.addDataSourceProperty("rewriteBatchedStatements", Boolean.TRUE.toString());
        config.addDataSourceProperty("cacheResultSetMetadata", Boolean.TRUE.toString());
        config.addDataSourceProperty("cacheServerConfiguration", Boolean.TRUE.toString());
        config.addDataSourceProperty("elideSetAutoCommits", Boolean.TRUE.toString());
        config.addDataSourceProperty("maintainTimeStats", Boolean.FALSE.toString());
        config.addDataSourceProperty("netTimeoutForStreamingResults", 0);
        return new HikariDataSource(config);
    }
}
