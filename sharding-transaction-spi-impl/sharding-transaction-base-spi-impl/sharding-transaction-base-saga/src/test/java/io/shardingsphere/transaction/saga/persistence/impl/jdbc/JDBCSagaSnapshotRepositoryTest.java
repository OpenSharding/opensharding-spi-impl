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

import io.shardingsphere.transaction.saga.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.revert.SQLRevertResult;
import lombok.SneakyThrows;

import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JDBCSagaSnapshotRepositoryTest {
    
    @Mock
    private DataSource dataSource;
    
    private JDBCSagaSnapshotRepository snapshotRepository;
    
    @Before
    @SneakyThrows
    public void setUp() {
        snapshotRepository = new JDBCSagaSnapshotRepository(dataSource, DatabaseType.H2);
        Field dataSourceField = JDBCSagaSnapshotRepository.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(snapshotRepository, dataSource);
    }
    
    @Test
    @SneakyThrows
    public void assertCreateTableIfNotExistsSuccess() {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        snapshotRepository.createTableIfNotExists();
        verify(statement, times(3)).executeUpdate(anyString());
    }
    
    @Test(expected = ShardingException.class)
    public void assertCreateTableIfNotExistsFailure() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeUpdate(anyString())).thenThrow(new SQLException("test execute fail"));
        snapshotRepository.createTableIfNotExists();
    }
    
    @Test
    @SneakyThrows
    public void assertInsert() {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        SagaSnapshot sagaSnapshot = mock(SagaSnapshot.class);
        when(sagaSnapshot.getTransactionContext()).thenReturn(mock(SagaBranchTransaction.class));
        when(sagaSnapshot.getRevertContext()).thenReturn(mock(SQLRevertResult.class));
        snapshotRepository.insert(sagaSnapshot);
        verify(statement).executeUpdate();
    }
    
    @Test
    @SneakyThrows
    public void assertDelete() {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        snapshotRepository.delete("1");
        verify(statement).executeUpdate();
    }
}
