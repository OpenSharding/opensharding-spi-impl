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

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.servicecomb.saga.core.JacksonToJsonFormat;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.apache.servicecomb.saga.core.ToJsonFormat;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * JDBC saga event repository.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Slf4j
public final class JDBCSagaEventRepository implements TableCreator {
    
    private static final String CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS running_sagas_index ON saga_event (saga_id, type)";
    
    private static final String INSERT_SQL = "INSERT INTO saga_event (saga_id, type, content_json) values (?, ?, ?)";

    private final DataSource dataSource;
    
    private final DatabaseType databaseType;
    
    private final ToJsonFormat toJsonFormat = new JacksonToJsonFormat();
    
    @Override
    public void createTableIfNotExists() {
        EventCreateTableSQL createTableSQL = new EventCreateTableSQL();
        try (Connection connection = dataSource.getConnection()) {
            JDBCUtil.executeUpdate(connection, createTableSQL.getCreateTableSQL(databaseType), Lists.newArrayList());
            createIndex(connection);
        } catch (SQLException ex) {
            throw new ShardingException("Create saga event persistence table failed", ex);
        }
    }
    
    private void createIndex(final Connection connection) throws SQLException {
        if (DatabaseType.MySQL != databaseType) {
            JDBCUtil.executeUpdate(connection, CREATE_INDEX_SQL, Lists.newArrayList());
        }
    }
    
    /**
     * Insert new saga event.
     *
     * @param sagaEvent saga event
     */
    public void insert(final SagaEvent sagaEvent) {
        try (Connection connection = dataSource.getConnection()) {
            JDBCUtil.executeUpdate(connection, INSERT_SQL, generateParams(sagaEvent));
        } catch (SQLException ex) {
            log.warn("Persist saga event failed", ex);
        }
    }
    
    private List<Object> generateParams(final SagaEvent sagaEvent) {
        List<Object> result = Lists.newArrayList();
        result.add(sagaEvent.sagaId);
        result.add(sagaEvent.getClass().getSimpleName());
        result.add(sagaEvent.json(toJsonFormat));
        return result;
    }
}
