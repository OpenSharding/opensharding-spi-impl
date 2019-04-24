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

package io.shardingsphere.transaction.base.saga.config;

import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public final class SagaConfigurationLoaderTest {
    
    @Test
    public void assertLoad() {
        SagaConfiguration sagaConfiguration = SagaConfigurationLoader.load();
        assertThat(sagaConfiguration.getExecutorSize(), is(16));
        assertThat(sagaConfiguration.getTransactionMaxRetries(), is(8));
        assertThat(sagaConfiguration.getCompensationMaxRetries(), is(4));
        assertThat(sagaConfiguration.getTransactionRetryDelayMilliseconds(), is(1000));
        assertThat(sagaConfiguration.getCompensationRetryDelayMilliseconds(), is(2000));
        assertThat(sagaConfiguration.getRecoveryPolicy(), is(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY));
        assertSagaPersistenceConfiguration(sagaConfiguration.getSagaPersistenceConfiguration());
    }
    
    private void assertSagaPersistenceConfiguration(final SagaPersistenceConfiguration sagaPersistenceConfiguration) {
        assertFalse(sagaPersistenceConfiguration.isEnablePersistence());
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
