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

package io.shardingsphere.transaction.saga.persistence.impl.jdbc;

import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;

/**
 * Create table SQL adapter.
 *
 * @author yangyi
 */
public abstract class AbstractCreateTableSQLAdapter {
    
    /**
     * Get create table sql.
     *
     * @param databaseType database type.
     * @return create table sql
     */
    public String getCreateTableSQL(final DatabaseType databaseType) {
        switch (databaseType) {
            case MySQL:
                return getMySQLCreateTableSQL();
            case H2:
                return getH2CreateTableSQL();
            case PostgreSQL:
                return getPostgreCreateTableSQL();
            default:
        }
        throw new ShardingException(String.format("Unsupported database type '%s'", databaseType.name()));
    }
    
    /**
     * Get create table SQL for MySQL.
     *
     * @return create table SQL
     */
    protected abstract String getMySQLCreateTableSQL();
    
    /**
     * Get create table SQL for MySQL and H2.
     *
     * @return create table SQL
     */
    protected abstract String getH2CreateTableSQL();
    
    /**
     * Get create table SQL for PostgreSQL.
     *
     * @return create table SQL
     */
    protected abstract String getPostgreCreateTableSQL();
}
