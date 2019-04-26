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

import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.apache.servicecomb.saga.core.ToJsonFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JDBCSagaEventRepositoryTest {
    
    @Mock
    private DataSource dataSource;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement statement;
    
    private JDBCSagaEventRepository eventRepository;
    
    @Before
    @SneakyThrows
    public void setUp() {
        eventRepository = new JDBCSagaEventRepository(dataSource);
        Field dataSourceField = JDBCSagaEventRepository.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(eventRepository, dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
    }
    
    @Test
    @SneakyThrows
    public void assertInsert() {
        SagaEvent sagaEvent = mock(SagaEvent.class);
        when(sagaEvent.json(any(ToJsonFormat.class))).thenReturn("{}");
        eventRepository.insert(sagaEvent);
        verify(statement).executeUpdate();
    }
}
