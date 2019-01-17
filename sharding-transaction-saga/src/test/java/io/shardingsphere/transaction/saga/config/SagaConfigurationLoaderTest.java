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

package io.shardingsphere.transaction.saga.config;

import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class SagaConfigurationLoaderTest {
    
    @Test
    public void assertLoad() {
        SagaConfiguration sagaConfiguration = SagaConfigurationLoader.load();
        assertThat(sagaConfiguration.getExecutorSize(), is(16));
        assertThat(sagaConfiguration.getTransactionMaxRetries(), is(8));
        assertThat(sagaConfiguration.getCompensationMaxRetries(), is(4));
        assertThat(sagaConfiguration.getTransactionRetryDelay(), is(1000));
        assertThat(sagaConfiguration.getCompensationRetryDelay(), is(2000));
        assertThat(sagaConfiguration.getRecoveryPolicy(), is(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY));
        assertSagaPersistenceConfiguration(sagaConfiguration.getSagaPersistenceConfiguration());
    }
    
    private void assertSagaPersistenceConfiguration(final SagaPersistenceConfiguration sagaPersistenceConfiguration) {
        assertTrue(sagaPersistenceConfiguration.isEnablePersistence());
        Map<String, String> dataSourceProperties = sagaPersistenceConfiguration.getDataSourceProperties();
        assertThat(dataSourceProperties.size(), is(7));
        assertThat(dataSourceProperties.get("url"), is("jdbc:mysql://localhost:3306/saga"));
        assertThat(dataSourceProperties.get("username"), is("root"));
        assertThat(dataSourceProperties.get("password"), is(""));
        assertThat(dataSourceProperties.get("type"), is("com.alibaba.druid.pool.DruidDataSource"));
        assertThat(dataSourceProperties.get("driver-class-name"), is("com.mysql.jdbc.Driver"));
        assertThat(dataSourceProperties.get("maxActive"), is("32"));
        assertThat(dataSourceProperties.get("minIdle"), is("5"));
    }
}
