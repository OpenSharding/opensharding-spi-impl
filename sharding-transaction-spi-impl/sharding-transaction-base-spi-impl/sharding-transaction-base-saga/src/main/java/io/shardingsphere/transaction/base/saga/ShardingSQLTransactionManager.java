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

package io.shardingsphere.transaction.base.saga;

import io.shardingsphere.transaction.base.context.ShardingSQLTransaction;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Sharding SQL transaction manager.
 *
 * @author zhaojun
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ShardingSQLTransactionManager {
    
    private static final ShardingSQLTransactionManager INSTANCE = new ShardingSQLTransactionManager();
    
    private static final ThreadLocal<ShardingSQLTransaction> CURRENT_TRANSACTION = new ThreadLocal<>();
    
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
     * Set sharding sQL transaction.
     *
     * @param shardingSQLTransaction sharding SQL transaction
     */
    public void set(final ShardingSQLTransaction shardingSQLTransaction) {
        CURRENT_TRANSACTION.set(shardingSQLTransaction);
    }
    
    /**
     * Clear sharding SQL transaction.
     */
    public static void clear() {
        CURRENT_TRANSACTION.remove();
    }
    
    /**
     * Whether current thread is in transaction or not.
     *
     * @return true or false
     */
    public boolean isInTransaction() {
        return null != getCurrentTransaction();
    }
}
