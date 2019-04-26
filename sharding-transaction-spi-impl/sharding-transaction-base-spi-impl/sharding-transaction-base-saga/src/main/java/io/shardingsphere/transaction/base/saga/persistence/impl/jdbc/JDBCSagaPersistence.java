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

package io.shardingsphere.transaction.base.saga.persistence.impl.jdbc;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.base.utils.JDBCUtil;
import org.apache.servicecomb.saga.core.EventEnvelope;
import org.apache.servicecomb.saga.core.PersistentStore;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.apache.shardingsphere.core.exception.ShardingException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC saga persistence.
 *
 * @author yangyi
 */
public final class JDBCSagaPersistence implements PersistentStore {
    
    private final DataSource dataSource;
    
    private final JDBCSagaEventRepository eventRepository;
    
    public JDBCSagaPersistence(final DataSource dataSource) {
        this.dataSource = dataSource;
        eventRepository = new JDBCSagaEventRepository(dataSource);
    }
    
    /**
     * Create table if not exists.
     */
    public void createTableIfNotExists() {
        Collection<String> sqls = SQLFileReader.readSQLs();
        if (0 == sqls.size()) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            for (String each : sqls) {
                JDBCUtil.executeUpdate(connection, each, Lists.newArrayList());
            }
        } catch (SQLException ex) {
            throw new ShardingException("Create saga persistence table failed", ex);
        }
    }
    
    @Override
    public Map<String, List<EventEnvelope>> findPendingSagaEvents() {
        return new HashMap<>(1);
    }
    
    @Override
    public void offer(final SagaEvent sagaEvent) {
        eventRepository.insert(sagaEvent);
    }
}
