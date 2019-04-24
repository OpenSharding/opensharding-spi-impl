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

package io.shardingsphere.transaction.saga.core.hook.revert.snapshot;

import io.shardingsphere.transaction.saga.core.hook.revert.GenericSQLBuilder;
import io.shardingsphere.transaction.saga.core.hook.revert.executor.SQLRevertExecutorContext;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * DML snapshot data accessor.
 *
 * @author zhaojun
 */
@RequiredArgsConstructor
public abstract class DMLSnapshotAccessor implements SnapshotAccessor {
    
    private GenericSQLBuilder sqlBuilder = new GenericSQLBuilder();
    
    @Getter
    private final SQLRevertExecutorContext executorContext;
    
    @Override
    public final List<Map<String, Object>> queryUndoData() throws SQLException {
        SnapshotSQLContext context = getSnapshotSQLContext(executorContext);
        return JDBCUtil.executeQuery(context.getConnection(), buildSnapshotQuerySQL(context), context.getParameters());
    }
    
    private String buildSnapshotQuerySQL(final SnapshotSQLContext context) {
        sqlBuilder.appendLiterals(DefaultKeyword.SELECT);
        sqlBuilder.appendColumns(context.getQueryColumnNames());
        sqlBuilder.appendLiterals(DefaultKeyword.FROM);
        sqlBuilder.appendLiterals(context.getTableName());
        sqlBuilder.appendLiterals(context.getTableAlias());
        sqlBuilder.appendLiterals(context.getWhereClause());
        return sqlBuilder.toSQL();
    }
    
    protected abstract SnapshotSQLContext getSnapshotSQLContext(SQLRevertExecutorContext context);
}
