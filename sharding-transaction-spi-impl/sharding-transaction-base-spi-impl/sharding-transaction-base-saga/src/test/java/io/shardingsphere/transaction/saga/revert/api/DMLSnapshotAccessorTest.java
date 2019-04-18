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

package io.shardingsphere.transaction.saga.revert.api;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DMLSnapshotAccessorTest {
    
    @Mock
    private SnapshotSQLStatement snapshotSQLStatement;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    @Mock
    private ResultSetMetaData resultSetMetaData;
    
    private DMLSnapshotAccessor dmlSnapshotAccessor;
    
    @Before
    public void setUp() throws SQLException {
        dmlSnapshotAccessor = new DMLSnapshotAccessor(snapshotSQLStatement, connection);
        when(snapshotSQLStatement.getActualTableName()).thenReturn("t_order_0");
        when(snapshotSQLStatement.getParameters()).thenReturn(Lists.newLinkedList());
        when(snapshotSQLStatement.getQueryColumnNames()).thenReturn(Collections.singleton("*"));
        when(snapshotSQLStatement.getTableAlias()).thenReturn("t");
        when(snapshotSQLStatement.getWhereClause()).thenReturn("where t.order_id=1");
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    }
    
    @Test
    public void assertQueryUndoData() throws SQLException {
        dmlSnapshotAccessor.queryUndoData();
        verify(connection).prepareStatement("SELECT * FROM t_order_0 t where t.order_id=1 ");
    }
    
    @Test
    public void assertQueryUndoDataCustomizeQueryItem() throws SQLException {
        when(snapshotSQLStatement.getQueryColumnNames()).thenReturn(Lists.newArrayList("order_id", "user_id", "status"));
        dmlSnapshotAccessor.queryUndoData();
        verify(connection).prepareStatement("SELECT order_id, user_id, status FROM t_order_0 t where t.order_id=1 ");
    }
}
