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

package io.shardingsphere.transaction.saga.hook;

import io.shardingsphere.transaction.saga.SagaShardingTransactionManager;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import org.apache.shardingsphere.core.routing.SQLUnit;
import org.apache.shardingsphere.core.routing.type.TableUnit;
import org.apache.shardingsphere.spi.hook.RewriteHook;

/**
 * Saga SQL rewrite hook.
 *
 * @author yangyi
 */
public final class SagaSQLRewriteHook implements RewriteHook {
    
    private final SagaTransaction sagaTransaction = SagaShardingTransactionManager.getCurrentTransaction();
    
    private TableUnit tableUnit;
    
    @Override
    public void start(final TableUnit tableUnit) {
        this.tableUnit = tableUnit;
    }
    
    @Override
    public void finishSuccess(final SQLUnit sqlUnit) {
        if (null != sagaTransaction) {
            sagaTransaction.getTableUnitMap().put(sqlUnit, tableUnit);
        }
    }
    
    @Override
    public void finishFailure(final Exception cause) {
    }
}
