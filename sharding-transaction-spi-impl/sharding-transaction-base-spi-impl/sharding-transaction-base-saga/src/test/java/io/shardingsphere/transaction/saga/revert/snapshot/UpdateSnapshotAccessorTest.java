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

package io.shardingsphere.transaction.saga.revert.snapshot;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.executor.SQLRevertExecutorContext;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.hamcrest.CoreMatchers;
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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UpdateSnapshotAccessorTest {
    
    @Mock
    private SQLRevertExecutorContext executorContext;
    
    @Mock
    private UpdateStatement updateStatement;
    
    @Mock
    private Tables tables;
    
    @Mock
    private Table table;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    @Mock
    private ResultSetMetaData resultSetMetaData;
    
    private List<Object> parameters = new LinkedList<>();
    
    private UpdateSnapshotAccessor updateSnapshotAccessor;
    
    @Before
    public void setUp() throws SQLException {
        when(executorContext.getActualTableName()).thenReturn("t_order_0");
        when(executorContext.getSqlStatement()).thenReturn(updateStatement);
        when(executorContext.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        updateSnapshotAccessor = new UpdateSnapshotAccessor(executorContext);
    }
    
    @Test
    public void assertGetSnapshotSQLContext() {
        String sql = "update t_order set status=? and modifier=? where order_id=? and user_id=?";
        setMockUpdateStatement(sql, "t_order", "", 43, 72, "status", "modifier");
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Lists.newArrayList("order_id"));
        SnapshotSQLContext actual = updateSnapshotAccessor.getSnapshotSQLContext(executorContext);
        assertThat(actual.getConnection(), is(connection));
        assertThat(actual.getParameters(), CoreMatchers.<Collection<Object>>is(parameters));
        assertThat(actual.getTableName(), is("t_order_0"));
        assertThat(actual.getWhereClause(), is("where order_id=? and user_id=?"));
        assertThat(actual.getTableAlias(), is(""));
        List<String> actualColumns = Lists.newArrayList(actual.getQueryColumnNames());
        assertThat(actualColumns, CoreMatchers.<List<String>>is(Lists.newArrayList("status", "modifier", "order_id")));
    }
    
    @Test
    public void assertGetSnapshotSQLContextWithTableAlias() {
    }
    
    @Test
    public void assertGetSnapshotSQLContextWithPrimaryKeyColumn() {
    }
    
    private void setMockUpdateStatement(final String logicSQL, final String tableName, final String tableAlias, final int whereStartIndex, final int whereStopIndex, final String... updateColumns) {
        when(updateStatement.getLogicSQL()).thenReturn(logicSQL);
        when(updateStatement.getAssignments()).thenReturn(mockUpdateAssignments(tableName, updateColumns));
        when(updateStatement.getWhereStartIndex()).thenReturn(whereStartIndex);
        when(updateStatement.getWhereStopIndex()).thenReturn(whereStopIndex);
        when(updateStatement.getTables()).thenReturn(tables);
        when(tables.getSingleTableName()).thenReturn(tableName);
        when(tables.find(tableName)).thenReturn(Optional.of(table));
        when(table.getAlias()).thenReturn(Optional.of(tableAlias));
        when(table.getName()).thenReturn(tableName);
    }
    
    private Map<Column, SQLExpression> mockUpdateAssignments(final String tableName, final String... columns) {
        Map<Column, SQLExpression> result = new LinkedHashMap<>();
        for (String each : columns) {
            result.put(new Column(each, tableName), mock(SQLExpression.class));
        }
        return result;
    }
}