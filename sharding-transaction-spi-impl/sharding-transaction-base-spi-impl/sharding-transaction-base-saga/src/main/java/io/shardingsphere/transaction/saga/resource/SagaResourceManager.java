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

import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.config.SagaConfigurationLoader;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaPersistenceLoader;
import io.shardingsphere.transaction.saga.servicecomb.SagaExecutionComponentFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;

/**
 * Saga resource manager.
 *
 * @author yangyi
 */
@Getter
public final class SagaResourceManager {
    
    private static final SagaResourceManager INSTANCE = new SagaResourceManager();
    
    private SagaConfiguration sagaConfiguration;
    
    private SagaPersistence sagaPersistence;
    
    private SagaExecutionComponent sagaExecutionComponent;
    
    private SagaResourceManager() {
        sagaConfiguration = SagaConfigurationLoader.load();
        sagaPersistence = SagaPersistenceLoader.load(sagaConfiguration.getSagaPersistenceConfiguration());
        sagaExecutionComponent = SagaExecutionComponentFactory.createSagaExecutionComponent(sagaConfiguration, sagaPersistence);
    }
    
    /**
     * Get instance.
     *
     * @return saga resource manager.
     */
    public static SagaResourceManager getInstance() {
        return INSTANCE;
    }
}
