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
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public final class JDBCSagaPersistenceTest {
    
    @Mock
    private JDBCSagaSnapshotRepository snapshotRepository;
    
    @Mock
    private JDBCSagaEventRepository eventRepository;
    
    private JDBCSagaPersistence sagaPersistence;
    
    @Before
    @SneakyThrows
    public void setUp() {
        sagaPersistence = new JDBCSagaPersistence(mock(DataSource.class), DatabaseType.MySQL);
        Field snapshotRepositoryField = JDBCSagaPersistence.class.getDeclaredField("snapshotRepository");
        snapshotRepositoryField.setAccessible(true);
        snapshotRepositoryField.set(sagaPersistence, snapshotRepository);
        Field eventRepositoryField = JDBCSagaPersistence.class.getDeclaredField("eventRepository");
        eventRepositoryField.setAccessible(true);
        eventRepositoryField.set(sagaPersistence, eventRepository);
    }
    
    @Test
    public void assertCreateTableIfNotExists() {
        sagaPersistence.createTableIfNotExists();
        verify(snapshotRepository).createTableIfNotExists();
        verify(eventRepository).createTableIfNotExists();
    }
    
    @Test
    public void assertPersistSnapshot() {
        SagaSnapshot snapshot = mock(SagaSnapshot.class);
        sagaPersistence.persistSnapshot(snapshot);
        verify(snapshotRepository).insert(snapshot);
    }
    
    @Test
    public void assertCleanSnapshot() {
        String transactionId = "1";
        sagaPersistence.cleanSnapshot(transactionId);
        verify(snapshotRepository).delete(transactionId);
    }
    
    @Test
    public void assertOffer() {
        SagaEvent sagaEvent = mock(SagaEvent.class);
        sagaPersistence.offer(sagaEvent);
        verify(eventRepository).insert(sagaEvent);
    }
    
    @Test
    public void assertFindPendingSagaEvents() {
        assertThat(sagaPersistence.findPendingSagaEvents().size(), is(0));
    }
}
