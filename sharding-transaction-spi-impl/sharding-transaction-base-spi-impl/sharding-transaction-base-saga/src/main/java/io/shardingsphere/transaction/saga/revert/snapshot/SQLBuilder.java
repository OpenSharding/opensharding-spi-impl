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

package io.shardingsphere.transaction.saga.revert.snapshot;

import com.google.common.base.Joiner;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.util.Collection;

/**
 * Snapshot query SQL builder.
 *
 * @author zhaojun
 */
public final class SQLBuilder {
    
    private StringBuilder sqlBuilder = new StringBuilder();
    
    /**
     * Append SQL literals.
     *
     * @param literals literals
     */
    public void appendLiterals(final Object literals) {
        if (null != literals) {
            sqlBuilder.append(literals).append(" ");
        }
    }
    
    /**
     * Append columns.
     *
     * @param columns column names
     */
    public void appendColumns(final Collection<String> columns) {
        sqlBuilder.append(Joiner.on(", ").join(columns)).append(" ");
    }
    
    /**
     * Append update set assignments.
     *
     * @param columns columns
     */
    public void appendUpdateSetAssignments(final Collection<String> columns) {
        appendLiterals(DefaultKeyword.SET);
        int index = 0;
        for (String each : columns) {
            sqlBuilder.append(each).append(" = ?");
            if (index < columns.size() - 1) {
                sqlBuilder.append(",");
            }
            index++;
        }
    }
    
    /**
     * Append where condition.
     *
     * @param columns columns
     */
    public void appendWhereCondition(final Collection<String> columns) {
        if (columns.isEmpty()) {
            return;
        }
        sqlBuilder.append(DefaultKeyword.WHERE).append(" ");
        boolean firstItem = true;
        for (String each : columns) {
            if (firstItem) {
                firstItem = false;
                sqlBuilder.append(each).append(" =?");
            } else {
                sqlBuilder.append(" ").append(DefaultKeyword.AND).append(" ").append(each).append(" =?");
            }
        }
    }
    
    /**
     * Append insert values.
     *
     * @param placeholderCount placeholder count
     */
    public void appendInsertValues(final int placeholderCount) {
        sqlBuilder.append(DefaultKeyword.VALUES).append(" ");
        sqlBuilder.append("(");
        for (int i = 0; i < placeholderCount; i++) {
            sqlBuilder.append("?");
            if (i < placeholderCount - 1) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(")");
    }
    
    /**
     * Generate SQL.
     *
     * @return snapshot query SQL
     */
    public String toSQL() {
        return sqlBuilder.toString();
    }
}
