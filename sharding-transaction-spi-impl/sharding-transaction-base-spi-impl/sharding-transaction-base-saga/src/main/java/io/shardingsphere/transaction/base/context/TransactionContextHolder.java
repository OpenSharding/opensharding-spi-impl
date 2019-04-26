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

package io.shardingsphere.transaction.base.context;

/**
 * Transaction context holder.
 *
 * @author zhaojun
 */
public class TransactionContextHolder {
    
    private static final ThreadLocal<TransactionContext> TRANSACTION_CONTEXT = new ThreadLocal<>();
    
    /**
     * Get transaction context of current thread.
     *
     * @return transaction context
     */
    public static TransactionContext get() {
        return TRANSACTION_CONTEXT.get();
    }
    
    /**
     * Set transaction context.
     *
     * @param transactionContext transaction context
     */
    public static void set(final TransactionContext transactionContext) {
        TRANSACTION_CONTEXT.set(transactionContext);
    }
    
    /**
     * Clear transaction context.
     */
    public static void clear() {
        TRANSACTION_CONTEXT.remove();
    }
    
    /**
     * Whether current thread is in transaction or not.
     *
     * @return true or false
     */
    public static boolean isInTransaction() {
        return null != get();
    }
}
