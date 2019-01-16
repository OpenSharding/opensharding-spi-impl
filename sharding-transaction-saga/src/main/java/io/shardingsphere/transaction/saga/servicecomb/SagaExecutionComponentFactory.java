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

package io.shardingsphere.transaction.saga.servicecomb;

import com.google.common.util.concurrent.MoreExecutors;
import io.shardingsphere.core.executor.ShardingThreadFactoryBuilder;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.servicecomb.transport.ShardingTransportFactory;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.servicecomb.saga.core.SagaDefinition;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;
import org.apache.servicecomb.saga.core.application.interpreter.FromJsonFormat;
import org.apache.servicecomb.saga.core.dag.GraphBasedSagaFactory;
import org.apache.servicecomb.saga.format.ChildrenExtractor;
import org.apache.servicecomb.saga.format.JacksonFromJsonFormat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Service comb saga execution component factory.
 *
 * @author yangyi
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SagaExecutionComponentFactory {
    
    /**
     * Create saga execution component.
     *
     * @param sagaConfiguration saga configuration
     * @param sagaPersistence saga persistence
     * @return saga execution component
     */
    public static SagaExecutionComponent createSagaExecutionComponent(final SagaConfiguration sagaConfiguration, final SagaPersistence sagaPersistence) {
        FromJsonFormat<SagaDefinition> fromJsonFormat = new JacksonFromJsonFormat(ShardingTransportFactory.getInstance());
        GraphBasedSagaFactory sagaFactory = new GraphBasedSagaFactory(
                sagaConfiguration.getCompensationRetryDelayMilliseconds(), sagaPersistence, new ChildrenExtractor(), createExecutorService(sagaConfiguration.getExecutorSize()));
        return new SagaExecutionComponent(sagaPersistence, fromJsonFormat, null, sagaFactory);
    }
    
    private static ExecutorService createExecutorService(final int executorSize) {
        ThreadFactory threadFactory = ShardingThreadFactoryBuilder.build("Saga-%d");
        ExecutorService result = executorSize > 0 ? Executors.newFixedThreadPool(executorSize, threadFactory) : Executors.newCachedThreadPool(threadFactory);
        MoreExecutors.addDelayedShutdownHook(result, 60, TimeUnit.SECONDS);
        return result;
    }
}
