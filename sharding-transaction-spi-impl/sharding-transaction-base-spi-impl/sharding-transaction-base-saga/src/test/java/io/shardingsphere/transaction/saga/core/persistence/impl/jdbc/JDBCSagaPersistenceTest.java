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

package io.shardingsphere.transaction.saga.core.persistence.impl.jdbc;

import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class JDBCSagaPersistenceTest {
    
    @Mock
    private JDBCSagaEventRepository eventRepository;
    
    @Mock
    private PreparedStatement statement;
    
    private JDBCSagaPersistence sagaPersistence;
    
    @Before
    @SneakyThrows
    public void setUp() {
        sagaPersistence = new JDBCSagaPersistence(mockDataSource());
        Field eventRepositoryField = JDBCSagaPersistence.class.getDeclaredField("eventRepository");
        eventRepositoryField.setAccessible(true);
        eventRepositoryField.set(sagaPersistence, eventRepository);
    }
    
    @SneakyThrows
    private DataSource mockDataSource() {
        DataSource result = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(result.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        return result;
    }
    
    @Test
    @SneakyThrows
    public void assertCreateTableIfNotExists() {
        sagaPersistence.createTableIfNotExists();
        verify(statement, times(4)).executeUpdate();
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
