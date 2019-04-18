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

package io.shardingsphere.transaction.saga.revert.api;

import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * DML snapshot data accessor.
 *
 * @author zhaojun
 */
public class DMLSnapshotAccessor implements SnapshotAccessor {
    
    private final SnapshotSQLStatement snapshotSQLStatement;
    
    private SQLBuilder sqlBuilder = new SQLBuilder();
    
    private final Connection connection;
    
    public DMLSnapshotAccessor(final SnapshotSQLStatement snapshotSQLStatement, final Connection connection) {
        this.snapshotSQLStatement = snapshotSQLStatement;
        this.connection = connection;
    }
    
    @Override
    public final List<Map<String, Object>> queryUndoData() throws SQLException {
        return JDBCUtil.executeQuery(connection, buildSnapshotQuerySQL(), snapshotSQLStatement.getParameters());
    }
    
    private String buildSnapshotQuerySQL() {
        sqlBuilder.appendLiterals(DefaultKeyword.SELECT);
        sqlBuilder.appendQueryColumnNames(snapshotSQLStatement.getQueryColumnNames());
        sqlBuilder.appendLiterals(DefaultKeyword.FROM);
        sqlBuilder.appendLiterals(snapshotSQLStatement.getActualTableName());
        sqlBuilder.appendLiterals(snapshotSQLStatement.getTableAlias());
        sqlBuilder.appendLiterals(snapshotSQLStatement.getWhereClause());
        return sqlBuilder.toSQL();
    }
}
