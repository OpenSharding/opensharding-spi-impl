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

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import org.apache.servicecomb.saga.core.EventEnvelope;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.apache.shardingsphere.core.constant.DatabaseType;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC saga persistence.
 *
 * @author yangyi
 */
public final class JDBCSagaPersistence implements SagaPersistence, TableCreator {
    
    private final JDBCSagaSnapshotRepository snapshotRepository;
    
    private final JDBCSagaEventRepository eventRepository;
    
    public JDBCSagaPersistence(final DataSource dataSource, final DatabaseType databaseType) {
        snapshotRepository = new JDBCSagaSnapshotRepository(dataSource, databaseType);
        eventRepository = new JDBCSagaEventRepository(dataSource, databaseType);
    }
    
    @Override
    public void createTableIfNotExists() {
        snapshotRepository.createTableIfNotExists();
        eventRepository.createTableIfNotExists();
    }
    
    @Override
    public void persistSnapshot(final SagaSnapshot snapshot) {
        snapshotRepository.insert(snapshot);
    }
    
    @Override
    public void updateSnapshotStatus(final String transactionId, final int snapshotId, final ExecuteStatus executeStatus) {
        snapshotRepository.update(transactionId, snapshotId, executeStatus);
    }
    
    @Override
    public void cleanSnapshot(final String transactionId) {
        snapshotRepository.delete(transactionId);
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
