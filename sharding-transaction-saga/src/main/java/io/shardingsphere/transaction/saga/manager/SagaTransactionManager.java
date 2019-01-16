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
import io.shardingsphere.transaction.core.ShardingTransactionManager;
import io.shardingsphere.transaction.saga.SagaTransaction;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.config.SagaConfigurationLoader;
import io.shardingsphere.transaction.saga.servicecomb.transport.ShardingTransportFactory;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.transaction.Status;

/**
 * Saga transaction manager.
 *
 * @author zhaojun
 * @author yangyi
 */
@Getter
public final class SagaTransactionManager implements ShardingTransactionManager {
    
    private static final String TRANSACTION_KEY = "transaction";
    
    private static final SagaTransactionManager INSTANCE = new SagaTransactionManager();
    
    private final ThreadLocal<SagaTransaction> transaction = new ThreadLocal<>();
    
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
    
    @Override
    public void begin() {
        if (null == transaction.get()) {
            SagaTransaction transaction = new SagaTransaction(sagaConfiguration, resourceManager.getSagaPersistence());
            ShardingExecuteDataMap.getDataMap().put(TRANSACTION_KEY, transaction);
            this.transaction.set(transaction);
            ShardingTransportFactory.getInstance().cacheTransport(transaction);
        }
    }
    
    @Override
    public void commit() {
        if (null != transaction.get() && transaction.get().isContainException()) {
            submitToActuator();
        }
        cleanTransaction();
    }
    
    @Override
    public void rollback() {
        if (null != transaction.get()) {
            submitToActuator();
        }
        cleanTransaction();
    }
    
    @SneakyThrows
    private void submitToActuator() {
        String json = transaction.get().getSagaDefinitionBuilder().build();
        resourceManager.getSagaExecutionComponent().run(json);
    }
    
    private void cleanTransaction() {
        if (null != transaction.get()) {
            transaction.get().cleanSnapshot();
        }
        ShardingTransportFactory.getInstance().remove();
        ShardingExecuteDataMap.getDataMap().remove(TRANSACTION_KEY);
        transaction.remove();
    }
    
    @Override
    public int getStatus() {
        return null == transaction.get() ? Status.STATUS_NO_TRANSACTION : Status.STATUS_ACTIVE;
    }
    
    /**
     * Get saga transaction object for current thread.
     *
     * @return saga transaction object
     */
    public SagaTransaction getTransaction() {
        return transaction.get();
    }
}
