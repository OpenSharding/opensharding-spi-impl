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

package io.shardingsphere.transaction.saga.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.shardingsphere.core.executor.ShardingExecuteDataMap;
import io.shardingsphere.transaction.saga.SagaTransaction;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.config.SagaConfigurationLoader;
import io.shardingsphere.transaction.saga.servicecomb.transport.ShardingTransportFactory;
import lombok.Getter;

import javax.transaction.Status;

/**
 * Saga transaction manager.
 *
 * @author zhaojun
 * @author yangyi
 */
@Getter
public final class SagaTransactionManager {
    
    private static final String TRANSACTION_KEY = "transaction";
    
    private static final SagaTransactionManager INSTANCE = new SagaTransactionManager();
    
    private static final ThreadLocal<SagaTransaction> TRANSACTION = new ThreadLocal<>();
    
    private final SagaConfiguration sagaConfiguration;
    
    private final SagaResourceManager resourceManager;
    
    private SagaTransactionManager() {
        sagaConfiguration = SagaConfigurationLoader.load();
        resourceManager = new SagaResourceManager(sagaConfiguration);
    }
    
    /**
     * Get instance of saga transaction manager.
     *
     * @return instance of saga transaction manager
     */
    public static SagaTransactionManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Begin transaction.
     */
    public void begin() {
        if (null == TRANSACTION.get()) {
            SagaTransaction transaction = new SagaTransaction(sagaConfiguration, resourceManager.getSagaPersistence());
            ShardingExecuteDataMap.getDataMap().put(TRANSACTION_KEY, transaction);
            TRANSACTION.set(transaction);
            ShardingTransportFactory.getInstance().cacheTransport(transaction);
        }
    }
    
    /**
     * Commit transaction.
     */
    public void commit() {
        if (null != TRANSACTION.get() && TRANSACTION.get().isContainException()) {
            submitToActuator();
        }
        cleanTransaction();
    }
    
    /**
     * Rollback transaction.
     */
    public void rollback() {
        if (null != TRANSACTION.get()) {
            submitToActuator();
        }
        cleanTransaction();
    }
    
    private void submitToActuator() {
        try {
            String json = TRANSACTION.get().getSagaDefinitionBuilder().build();
            resourceManager.getSagaExecutionComponent().run(json);
        } catch (JsonProcessingException ignored) {
        }
    }
    
    private void cleanTransaction() {
        if (null != TRANSACTION.get()) {
            TRANSACTION.get().cleanSnapshot();
        }
        ShardingTransportFactory.getInstance().remove();
        ShardingExecuteDataMap.getDataMap().remove(TRANSACTION_KEY);
        TRANSACTION.remove();
    }
    
    /**
     * Get transaction status.
     * 
     * @return transaction status, {@see javax.transaction.Status}
     */
    public int getStatus() {
        return null == TRANSACTION.get() ? Status.STATUS_NO_TRANSACTION : Status.STATUS_ACTIVE;
    }
    
    /**
     * Get saga transaction object for current thread.
     *
     * @return saga transaction object
     */
    public SagaTransaction getTransaction() {
        return TRANSACTION.get();
    }
}
