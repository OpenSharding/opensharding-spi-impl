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
        assertThat(sagaPersistenceConfiguration.getUrl(), is("jdbc:mysql://localhost:3306/saga"));
        assertThat(sagaPersistenceConfiguration.getUsername(), is("root"));
        assertThat(sagaPersistenceConfiguration.getPassword(), is(""));
        assertThat(sagaPersistenceConfiguration.getMaxPoolSize(), is(32));
        assertThat(sagaPersistenceConfiguration.getMinPoolSize(), is(4));
        assertThat(sagaPersistenceConfiguration.getConnectionTimeoutMilliseconds(), is(30000L));
        assertThat(sagaPersistenceConfiguration.getIdleTimeoutMilliseconds(), is(60000L));
        assertThat(sagaPersistenceConfiguration.getMaintenanceIntervalMilliseconds(), is(29999L));
        assertThat(sagaPersistenceConfiguration.getMaxLifetimeMilliseconds(), is(1800000L));
    }
}
