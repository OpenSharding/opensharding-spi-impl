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

package io.shardingsphere.transaction.saga.core.resource.persistence.impl.jdbc;

import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SQLFileReaderTest {
    
    private static final String SNAPSHOT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_snapshot("
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        + "transaction_id VARCHAR(255) null,"
        + "snapshot_id int null,"
        + "revert_context VARCHAR(255) null,"
        + "transaction_context VARCHAR(255) null,"
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        + ")";
    
    private static final String SNAPSHOT_CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS transaction_snapshot_index ON saga_snapshot(transaction_id, snapshot_id)";
    
    private static final String EVENT_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS saga_event("
        + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        + "saga_id VARCHAR(255) null,"
        + "type VARCHAR(255) null,"
        + "content_json TEXT null,"
        + "create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        + ")";
    
    private static final String EVENT_CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS running_sagas_index ON saga_event (saga_id, type)";
    
    @Test
    public void readSQLs() throws Exception {
        Collection<String> sqls = SQLFileReader.readSQLs();
        assertThat(sqls.size(), is(4));
        Iterator<String> sqlIterator = sqls.iterator();
        assertThat(sqlIterator.next(), is(SNAPSHOT_CREATE_TABLE_SQL));
        assertThat(sqlIterator.next(), is(SNAPSHOT_CREATE_INDEX_SQL));
        assertThat(sqlIterator.next(), is(EVENT_CREATE_TABLE_SQL));
        assertThat(sqlIterator.next(), is(EVENT_CREATE_INDEX_SQL));
    }
}
