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

package io.opensharding.transaction.base.saga;

import io.opensharding.transaction.base.context.ShardingSQLTransaction;
import io.opensharding.transaction.base.saga.actuator.SagaActuatorFactory;
import io.opensharding.transaction.base.saga.actuator.definition.SagaDefinitionFactory;
import io.opensharding.transaction.base.saga.config.SagaConfiguration;
import io.opensharding.transaction.base.saga.config.SagaConfigurationLoader;
import io.opensharding.transaction.base.saga.persistence.SagaPersistenceLoader;
import io.opensharding.transaction.base.utils.Constant;
import org.apache.servicecomb.saga.core.PersistentStore;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;
import org.apache.shardingsphere.core.execute.ShardingExecuteDataMap;
import org.apache.shardingsphere.transaction.core.TransactionOperationType;

/**
 * Sharding SQL transaction manager.
 *
 * @author zhaojun
 */
public final class ShardingSQLTransactionManager {
    
    private static final ShardingSQLTransactionManager INSTANCE = new ShardingSQLTransactionManager();
    
    private static final ThreadLocal<ShardingSQLTransaction> CURRENT_TRANSACTION = new ThreadLocal<>();
    
    private SagaConfiguration sagaConfiguration;
    
    private SagaExecutionComponent sagaActuator;
    
    private ShardingSQLTransactionManager() {
        sagaConfiguration = SagaConfigurationLoader.load();
        PersistentStore sagaPersistence = SagaPersistenceLoader.load(sagaConfiguration.getSagaPersistenceConfiguration());
        sagaActuator = SagaActuatorFactory.newInstance(sagaConfiguration, sagaPersistence);
    }
    
    
    /**
     * Get instance of Sharding SQL transaction manager.
     *
     * @return Sharding SQL transaction manager
     */
    public static ShardingSQLTransactionManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Get current sharding SQL transaction.
     *
     * @return transaction context
     */
    public ShardingSQLTransaction getCurrentTransaction() {
        return CURRENT_TRANSACTION.get();
    }
    
    /**
     * begin.
     */
    public void begin() {
        if (!isInTransaction()) {
            CURRENT_TRANSACTION.set(new ShardingSQLTransaction());
            ShardingExecuteDataMap.getDataMap().put(Constant.SAGA_TRANSACTION_KEY, getCurrentTransaction());
        }
    }
    
    /**
     * commit.
     */
    public void commit() {
        try {
            if (isInTransaction() && getCurrentTransaction().isContainsException()) {
                getCurrentTransaction().setOperationType(TransactionOperationType.COMMIT);
                sagaActuator.run(SagaDefinitionFactory.newInstance(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY, sagaConfiguration, getCurrentTransaction()).toJson());
            }
        } finally {
            clear();
        }
        
    }
    
    /**
     * rollback.
     */
    public void rollback() {
        try {
            if (isInTransaction()) {
                getCurrentTransaction().setOperationType(TransactionOperationType.ROLLBACK);
                sagaActuator.run(SagaDefinitionFactory.newInstance(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY, sagaConfiguration, getCurrentTransaction()).toJson());
            }
        } finally {
            clear();
        }
    }
    
    /**
     * Whether current thread is in transaction or not.
     *
     * @return true or false
     */
    public boolean isInTransaction() {
        return null != getCurrentTransaction();
    }
    
    /**
     * clear.
     */
    public void clear() {
        CURRENT_TRANSACTION.remove();
        ShardingExecuteDataMap.getDataMap().remove(Constant.SAGA_TRANSACTION_KEY);
    }
}
