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
     * Append Query column names.
     *
     * @param columnNames column names
     */
    public void appendQueryColumnNames(final Collection<String> columnNames) {
        boolean firstItem = true;
        for (String each : columnNames) {
            if (firstItem) {
                sqlBuilder.append(each);
                firstItem = false;
            } else {
                sqlBuilder.append(",").append(" ").append(each);
            }
        }
        sqlBuilder.append(" ");
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
