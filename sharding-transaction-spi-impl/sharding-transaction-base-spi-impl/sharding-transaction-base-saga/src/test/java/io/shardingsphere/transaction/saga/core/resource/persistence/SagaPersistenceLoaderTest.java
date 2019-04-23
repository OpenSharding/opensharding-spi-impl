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

package io.shardingsphere.transaction.saga.core.resource.persistence;

import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import io.shardingsphere.transaction.saga.core.resource.config.SagaPersistenceConfiguration;
import io.shardingsphere.transaction.saga.core.resource.persistence.impl.EmptySagaPersistence;
import io.shardingsphere.transaction.saga.core.resource.persistence.impl.jdbc.JDBCSagaPersistence;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public final class SagaPersistenceLoaderTest {
    
    @Test
    public void assertLoadDisabledPersistence() {
        SagaPersistenceConfiguration persistenceConfiguration = new SagaPersistenceConfiguration();
        assertThat(SagaPersistenceLoader.load(persistenceConfiguration), instanceOf(EmptySagaPersistence.class));
    }
    
    @Test(expected = PoolInitializationException.class)
    public void assertLoadDefaultPersistenceWithMySQL() {
        SagaPersistenceConfiguration persistenceConfiguration = new SagaPersistenceConfiguration();
        persistenceConfiguration.setEnablePersistence(true);
        persistenceConfiguration.setUrl("jdbc:mysql://localhost:3306/saga");
        SagaPersistenceLoader.load(persistenceConfiguration);
    }
    
    @Test
    public void assertLoadDefaultPersistenceWithH2() {
        SagaPersistenceConfiguration persistenceConfiguration = new SagaPersistenceConfiguration();
        persistenceConfiguration.setEnablePersistence(true);
        persistenceConfiguration.setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;");
        assertThat(SagaPersistenceLoader.load(persistenceConfiguration), instanceOf(JDBCSagaPersistence.class));
    }
    
    @Test(expected = ShardingException.class)
    public void assertWithoutJDBCDriver() {
        SagaPersistenceConfiguration persistenceConfiguration = new SagaPersistenceConfiguration();
        persistenceConfiguration.setEnablePersistence(true);
        persistenceConfiguration.setUrl("jdbc:postgresql://localhost:5432/saga");
        SagaPersistenceLoader.load(persistenceConfiguration);
    }
}
