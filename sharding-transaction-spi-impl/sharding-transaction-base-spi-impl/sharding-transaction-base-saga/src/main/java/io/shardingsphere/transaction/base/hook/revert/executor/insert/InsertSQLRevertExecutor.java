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

package io.shardingsphere.transaction.base.hook.revert.executor.insert;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.shardingsphere.transaction.base.hook.revert.GenericSQLBuilder;
import io.shardingsphere.transaction.base.hook.revert.RevertSQLResult;
import io.shardingsphere.transaction.base.hook.revert.constant.DefaultKeyword;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutor;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.optimize.sharding.statement.dml.ShardingInsertOptimizedStatement;

import java.util.Map;

/**
 * Insert SQL revert executor.
 *
 * @author duhongjun
 * @author zhaojun
 */
@RequiredArgsConstructor
public final class InsertSQLRevertExecutor implements SQLRevertExecutor {
    
    private InsertSQLRevertContext sqlRevertContext;
    
    private GenericSQLBuilder sqlBuilder = new GenericSQLBuilder();
    
    public InsertSQLRevertExecutor(final InsertSQLRevertContext revertContext) {
        sqlRevertContext = revertContext;
    }
    
    public InsertSQLRevertExecutor(final SQLRevertExecutorContext executorContext) {
        sqlRevertContext = new InsertSQLRevertContext(executorContext.getDataSourceName(), executorContext.getActualTableName(),
            executorContext.getPrimaryKeyColumns(), (ShardingInsertOptimizedStatement) executorContext.getOptimizedStatement());
    }
    
    @Override
    public Optional<String> revertSQL() {
        Preconditions.checkState(!sqlRevertContext.getPrimaryKeyInsertValues().isEmpty(),
            "Could not found primary key values. datasource:[%s], table:[%s]", sqlRevertContext.getDataSourceName(), sqlRevertContext.getActualTable());
        sqlBuilder.appendLiterals(DefaultKeyword.DELETE);
        sqlBuilder.appendLiterals(DefaultKeyword.FROM);
        sqlBuilder.appendLiterals(sqlRevertContext.getActualTable());
        sqlBuilder.appendWhereCondition(sqlRevertContext.getPrimaryKeyInsertValues().iterator().next().keySet());
        return Optional.of(sqlBuilder.toSQL());
    }
    
    @Override
    public void fillParameters(final RevertSQLResult revertSQLResult) {
        for (Map<String, Object> each : sqlRevertContext.getPrimaryKeyInsertValues()) {
            revertSQLResult.getParameters().add(each.values());
        }
    }
}
