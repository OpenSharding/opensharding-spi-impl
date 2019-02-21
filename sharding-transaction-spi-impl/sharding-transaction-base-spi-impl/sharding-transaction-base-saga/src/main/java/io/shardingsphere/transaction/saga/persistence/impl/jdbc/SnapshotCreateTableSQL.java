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

/**
 * Snapshot Create Table SQL generator.
 *
 * @author yangyi
 */
public final class SnapshotCreateTableSQL extends AbstractCreateTableSQLAdapter {
    
    private static final String MYSQL_SNAPSHOT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_snapshot("
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        + "transaction_id VARCHAR(255) null, "
        + "snapshot_id int null, "
        + "revert_context VARCHAR(255) null, "
        + "transaction_context VARCHAR(255) null, "
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        + "INDEX transaction_snapshot_index(transaction_id, snapshot_id))";
    
    private static final String H2_SNAPSHOT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_snapshot("
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        + "transaction_id VARCHAR(255) null, "
        + "snapshot_id int null, "
        + "revert_context VARCHAR(255) null, "
        + "transaction_context VARCHAR(255) null, "
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
    
    private static final String POSTGRE_SNAPSHOT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_snapshot("
        + "id BIGSERIAL PRIMARY KEY, "
        + "transaction_id VARCHAR(255) null, "
        + "snapshot_id int null, "
        + "revert_context VARCHAR(255) null, "
        + "transaction_context VARCHAR(255) null, "
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
    
    @Override
    protected String getMySQLCreateTableSQL() {
        return MYSQL_SNAPSHOT_CREATE_TABLE_SQL;
    }
    
    @Override
    protected String getH2CreateTableSQL() {
        return H2_SNAPSHOT_CREATE_TABLE_SQL;
    }
    
    @Override
    protected String getPostgreCreateTableSQL() {
        return POSTGRE_SNAPSHOT_CREATE_TABLE_SQL;
    }
}
