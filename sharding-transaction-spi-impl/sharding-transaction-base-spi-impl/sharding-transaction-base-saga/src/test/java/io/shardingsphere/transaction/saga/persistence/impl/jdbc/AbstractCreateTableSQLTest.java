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
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public abstract class AbstractCreateTableSQLTest {
    
    @Test
    public void assertGetCreateTableSQL() {
        AbstractCreateTableSQLAdapter createTableSQL = getCreateTableSQLAdapter();
        assertTrue(createTableSQL.getCreateTableSQL(DatabaseType.MySQL).contains("INDEX"));
        assertTrue(createTableSQL.getCreateTableSQL(DatabaseType.MySQL).contains("BIGINT AUTO_INCREMENT"));
        assertTrue(createTableSQL.getCreateTableSQL(DatabaseType.H2).contains("BIGINT AUTO_INCREMENT"));
        assertTrue(createTableSQL.getCreateTableSQL(DatabaseType.PostgreSQL).contains("BIGSERIAL"));
    }
    
    @Test(expected = ShardingException.class)
    public void assertGetCreateTableSQLFailure() {
        AbstractCreateTableSQLAdapter createTableSQL = getCreateTableSQLAdapter();
        createTableSQL.getCreateTableSQL(DatabaseType.Oracle);
    }
    
    protected abstract AbstractCreateTableSQLAdapter getCreateTableSQLAdapter();
}
