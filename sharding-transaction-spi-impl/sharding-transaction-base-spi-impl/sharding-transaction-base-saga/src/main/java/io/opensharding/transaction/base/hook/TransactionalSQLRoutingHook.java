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

package io.opensharding.transaction.base.hook;

import io.opensharding.transaction.base.saga.ShardingSQLTransactionManager;
import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.hook.RoutingHook;

/**
 * Transactional SQL routing hook.
 *
 * @author zhaojun
 */
public final class TransactionalSQLRoutingHook implements RoutingHook {
    
    private final ShardingSQLTransactionManager shardingSQLTransactionManager = ShardingSQLTransactionManager.getInstance();
    
    @Override
    public void start(final String sql) {
        if (shardingSQLTransactionManager.isInTransaction()) {
            shardingSQLTransactionManager.getCurrentTransaction().nextLogicSQLTransaction(sql);
        }
    }
    
    @Override
    public void finishSuccess(final SQLRouteResult sqlRouteResult, final TableMetas tableMetas) {
        if (shardingSQLTransactionManager.isInTransaction()) {
            shardingSQLTransactionManager.getCurrentTransaction().initLogicSQLTransaction(sqlRouteResult, tableMetas);
        }
    }
    
    @Override
    public void finishFailure(final Exception cause) {
    }
}
