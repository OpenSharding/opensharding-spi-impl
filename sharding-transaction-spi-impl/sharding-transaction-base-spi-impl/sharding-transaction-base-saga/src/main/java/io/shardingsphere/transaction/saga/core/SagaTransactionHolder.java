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

package io.shardingsphere.transaction.saga.core;

import io.shardingsphere.transaction.saga.core.context.SagaTransaction;

/**
 * Saga transaction holder.
 *
 * @author zhaojun
 */
public class SagaTransactionHolder {
    
    private static final ThreadLocal<SagaTransaction> SAGA_TRANSACTION = new ThreadLocal<>();
    
    /**
     * Get saga transaction for current thread.
     *
     * @return saga transaction
     */
    public static SagaTransaction get() {
        return SAGA_TRANSACTION.get();
    }
    
    /**
     * Set saga transaction.
     *
     * @param sagaTransaction saga transaction
     */
    public static void set(final SagaTransaction sagaTransaction) {
        SAGA_TRANSACTION.set(sagaTransaction);
    }
    
    /**
     * Clear saga transaction.
     */
    public static void clear() {
        SAGA_TRANSACTION.remove();
    }
    
    /**
     * Whether current thread is in saga transaction or not.
     *
     * @return true or false
     */
    public static boolean isInTransaction() {
        return null != get();
    }
}
