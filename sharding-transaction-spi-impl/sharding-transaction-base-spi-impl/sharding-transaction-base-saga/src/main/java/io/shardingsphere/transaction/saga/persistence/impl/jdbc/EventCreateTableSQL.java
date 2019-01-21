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
 * Event create table SQL generator.
 *
 * @author yangyi
 */
public final class EventCreateTableSQL extends AbstractCreateTableSQLAdapter {
    
    private static final String COMMON_EVENT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_event( "
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
        + "saga_id VARCHAR(255) null, "
        + "type VARCHAR(255) null, "
        + "content_json TEXT null, "
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
    
    private static final String POSTGRE_EVENT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_event( "
        + "id BIGSERIAL PRIMARY KEY, "
        + "saga_id VARCHAR(255) null, "
        + "type VARCHAR(255) null, "
        + "content_json TEXT null, "
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
    
    @Override
    protected String getCommonCreateTableSQL() {
        return COMMON_EVENT_CREATE_TABLE_SQL;
    }
    
    @Override
    protected String getPostgreCreateTableSQL() {
        return POSTGRE_EVENT_CREATE_TABLE_SQL;
    }
}
