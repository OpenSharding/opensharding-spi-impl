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

import lombok.Getter;
import lombok.Setter;
import org.apache.servicecomb.saga.core.RecoveryPolicy;

/**
 * Saga configuration.
 *
 * @author yangyi
 */
@Getter
@Setter
public final class SagaConfiguration {
    
    private int executorSize = 5;
    
    private int transactionMaxRetries = 5;
    
    private int compensationMaxRetries = 3;
    
    private int transactionRetryDelayMilliseconds = 5000;
    
    private int compensationRetryDelayMilliseconds = 3000;
    
    private String recoveryPolicy = RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY;
    
    private boolean enablePersistence;
}
