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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * DML snapshot data accessor.
 *
 * @author zhaojun
 */
public abstract class DMLSnapshotDataAccessor implements SnapshotDataAccessor {
    
    private final String actualTableName;
    
    private SnapshotQuerySQLBuilder sqlBuilder = new SnapshotQuerySQLBuilder();
    
    protected DMLSnapshotDataAccessor(final String actualTableName) {
        this.actualTableName = actualTableName;
    }
    
    @Override
    public final Collection<Map<String, Object>> queryUndoData(final Connection connection) throws SQLException {
        return JDBCUtil.executeQuery(connection, createSnapshotQuerySQL(), getQueryParameters());
    }
    
    private String createSnapshotQuerySQL() {
        sqlBuilder.appendLiterals(DefaultKeyword.SELECT);
        sqlBuilder.appendQueryItems(Collections.singletonList("*"));
        sqlBuilder.appendLiterals(DefaultKeyword.FROM);
        sqlBuilder.appendLiterals(actualTableName);
        sqlBuilder.appendLiterals(getTableAliasLiterals());
        sqlBuilder.appendLiterals(getWhereClauseLiterals());
        return sqlBuilder.toSQL();
    }
    
    /**
     * Get Table alias literals.
     *
     * @return table alias
     */
    public abstract String getTableAliasLiterals();
    
    /**
     * Get where clause literals.
     *
     * @return where clause literals
     */
    public abstract String getWhereClauseLiterals();
    
    /**
     * Get Query parameters.
     *
     * @return Collection
     */
    public abstract Collection<Object> getQueryParameters();
}

