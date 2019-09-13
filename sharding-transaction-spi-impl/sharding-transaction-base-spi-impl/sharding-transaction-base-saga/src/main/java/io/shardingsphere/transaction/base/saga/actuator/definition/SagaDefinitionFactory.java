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

package io.shardingsphere.transaction.base.saga.actuator.definition;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.base.context.SQLTransaction;
import io.shardingsphere.transaction.base.context.LogicSQLTransaction;
import io.shardingsphere.transaction.base.context.ShardingSQLTransaction;
import io.shardingsphere.transaction.base.saga.config.SagaConfiguration;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.servicecomb.saga.core.RecoveryPolicy;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Saga definition factory.
 *
 * @author zhaojun
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SagaDefinitionFactory {
    
    public static final String ROLLBACK_TAG = "rollbackTag";
    
    private static final String TYPE = "sql";
    
    /**
     * New instance of saga definition.
     *
     * @param recoveryPolicy recovery policy
     * @param configuration configuration
     * @param shardingSQLTransaction sharding SQL transaction
     * @return saga definition
     */
    public static SagaDefinition newInstance(final String recoveryPolicy, final SagaConfiguration configuration, final ShardingSQLTransaction shardingSQLTransaction) {
        Collection<SagaRequest> sagaRequests = new LinkedList<>();
        Collection<String> requestIds = new LinkedList<>();
        for (LogicSQLTransaction each : shardingSQLTransaction.getLogicSQLTransactions()) {
            requestIds = addLogicSQLTransactionRequest(new LinkedList<>(requestIds), sagaRequests, each, configuration);
        }
        if (RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(recoveryPolicy)) {
            sagaRequests.add(newRollbackRequest(new LinkedList<>(requestIds), configuration));
        }
        return new SagaDefinition(recoveryPolicy, sagaRequests);
    }
    
    private static Collection<String> addLogicSQLTransactionRequest(final Collection<String> parentsIds, final Collection<SagaRequest> sagaRequests,
                                                                    final LogicSQLTransaction logicSQLTransaction, final SagaConfiguration configuration) {
        Collection<String> result = new LinkedList<>();
        for (SQLTransaction each : logicSQLTransaction.getSqlTransactions()) {
            sagaRequests.add(newSagaRequest(parentsIds, each, configuration));
            result.add(each.getSqlTransactionId());
        }
        return result;
    }
    
    private static SagaRequest newSagaRequest(final Collection<String> parentsIds, final SQLTransaction sqlTransaction, final SagaConfiguration configuration) {
        SagaSQLUnit transaction = new SagaSQLUnit(sqlTransaction.getSql(), sqlTransaction.getParameters(), configuration.getTransactionMaxRetries());
        SagaSQLUnit compensation = new SagaSQLUnit(sqlTransaction.getRevertSQLResult().getSql(), sqlTransaction.getRevertSQLResult().getParameters(), configuration.getCompensationMaxRetries());
        return new SagaRequest(sqlTransaction.getSqlTransactionId(), sqlTransaction.getDataSourceName(), TYPE, transaction, compensation, parentsIds, configuration.getTransactionRetryDelayMilliseconds());
    }
    
    private static SagaRequest newRollbackRequest(final Collection<String> parentsIds, final SagaConfiguration configuration) {
        SagaSQLUnit transaction = new SagaSQLUnit(ROLLBACK_TAG, Lists.<Collection<Object>>newLinkedList(), configuration.getTransactionMaxRetries());
        SagaSQLUnit compensation = new SagaSQLUnit(ROLLBACK_TAG, Lists.<Collection<Object>>newLinkedList(), configuration.getCompensationMaxRetries());
        return new SagaRequest(ROLLBACK_TAG, ROLLBACK_TAG, TYPE, transaction, compensation, parentsIds, configuration.getTransactionRetryDelayMilliseconds());
    }
}
