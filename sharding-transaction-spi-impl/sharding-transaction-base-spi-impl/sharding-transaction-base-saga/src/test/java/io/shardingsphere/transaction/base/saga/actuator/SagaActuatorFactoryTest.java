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

package io.shardingsphere.transaction.base.saga.actuator;

import io.shardingsphere.transaction.base.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.base.saga.persistence.impl.EmptySagaPersistence;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;
import org.apache.servicecomb.saga.core.dag.GraphBasedSagaFactory;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadPoolExecutor;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class SagaActuatorFactoryTest {
    
    @Test
    public void assertCreateSagaExecutionComponent() throws NoSuchFieldException, IllegalAccessException {
        assertCreateWithFixedExecutors();
        assertCreateWithCachedExecutors();
    }
    
    private void assertCreateWithFixedExecutors() throws NoSuchFieldException, IllegalAccessException {
        SagaConfiguration sagaConfiguration = new SagaConfiguration();
        SagaExecutionComponent sagaExecutionComponent = SagaActuatorFactory.newInstance(sagaConfiguration, new EmptySagaPersistence());
        assertThat(sagaExecutionComponent, instanceOf(SagaExecutionComponent.class));
        ThreadPoolExecutor threadPoolExecutor = getExecutorFromComponent(sagaExecutionComponent);
        assertThat(threadPoolExecutor.getCorePoolSize(), is(sagaConfiguration.getExecutorSize()));
        assertThat(threadPoolExecutor.getMaximumPoolSize(), is(sagaConfiguration.getExecutorSize()));
    }
    
    private void assertCreateWithCachedExecutors() throws NoSuchFieldException, IllegalAccessException {
        SagaConfiguration sagaConfiguration = new SagaConfiguration();
        sagaConfiguration.setExecutorSize(0);
        SagaExecutionComponent sagaExecutionComponent = SagaActuatorFactory.newInstance(sagaConfiguration, new EmptySagaPersistence());
        assertThat(sagaExecutionComponent, instanceOf(SagaExecutionComponent.class));
        ThreadPoolExecutor threadPoolExecutor = getExecutorFromComponent(sagaExecutionComponent);
        assertThat(threadPoolExecutor.getCorePoolSize(), is(0));
        assertThat(threadPoolExecutor.getMaximumPoolSize(), is(Integer.MAX_VALUE));
    }
    
    private ThreadPoolExecutor getExecutorFromComponent(final SagaExecutionComponent sagaExecutionComponent) throws NoSuchFieldException, IllegalAccessException {
        Field sagaFactoryField = SagaExecutionComponent.class.getDeclaredField("sagaFactory");
        sagaFactoryField.setAccessible(true);
        GraphBasedSagaFactory sagaFactory = (GraphBasedSagaFactory) sagaFactoryField.get(sagaExecutionComponent);
        Field executorServiceField = GraphBasedSagaFactory.class.getDeclaredField("executorService");
        executorServiceField.setAccessible(true);
        return (ThreadPoolExecutor) executorServiceField.get(sagaFactory);
    }
}
