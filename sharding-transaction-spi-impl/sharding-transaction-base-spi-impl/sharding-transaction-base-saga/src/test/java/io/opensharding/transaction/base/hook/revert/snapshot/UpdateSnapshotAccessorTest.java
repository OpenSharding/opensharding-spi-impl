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

package io.opensharding.transaction.base.hook.revert.snapshot;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.opensharding.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import org.apache.shardingsphere.core.optimize.api.segment.Table;
import org.apache.shardingsphere.core.optimize.api.segment.Tables;
import org.apache.shardingsphere.core.optimize.sharding.statement.ShardingOptimizedStatement;
import org.apache.shardingsphere.core.parse.sql.segment.dml.assignment.AssignmentSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.assignment.SetAssignmentsSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.core.parse.sql.segment.generic.TableSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.UpdateStatement;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UpdateSnapshotAccessorTest {
    
    @Mock
    private SQLRevertExecutorContext executorContext;
    
    @Mock
    private ShardingOptimizedStatement shardingOptimizedStatement;
    
    @Spy
    private UpdateStatement updateStatement = new UpdateStatement();
    
    @Mock
    private TableSegment tableSegment;
    
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
        parameters.addAll(Arrays.asList(1, 2));
        when(executorContext.getParameters()).thenReturn(parameters);
        when(executorContext.getActualTableName()).thenReturn("t_order_0");
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Lists.newArrayList("order_id"));
        when(executorContext.getShardingStatement()).thenReturn(shardingOptimizedStatement);
        when(shardingOptimizedStatement.getSQLStatement()).thenReturn(updateStatement);
        when(executorContext.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        updateSnapshotAccessor = new UpdateSnapshotAccessor(executorContext);
    }
    
    @Test
    public void assertGetSnapshotSQLContext() {
        String sql = "update t_order set status=?, modifier=? where order_id=? and user_id=?";
        setMockUpdateStatement(sql, "t_order", "", 40, 69, "status", "modifier");
        SnapshotSQLContext actual = updateSnapshotAccessor.getSnapshotSQLContext(executorContext);
        assertThat(actual.getConnection(), is(connection));
        assertThat(actual.getParameters().size(), is(2));
        assertThat(actual.getTableName(), is("t_order_0"));
        assertThat(actual.getWhereClause(), is("where order_id=? and user_id=?"));
        assertThat(actual.getTableAlias(), is(""));
        List<String> actualColumns = Lists.newArrayList(actual.getQueryColumnNames());
        assertThat(actualColumns, CoreMatchers.<List<String>>is(Lists.newArrayList("status", "modifier", "order_id")));
    }
    
    @Test
    public void assertGetSnapshotSQLContextWithTableAlias() {
        String sql = "update t_order t set t.status=?, t.modifier=? where t.order_id=? and t.user_id=?";
        setMockUpdateStatement(sql, "t_order", "t", 46, 79, "status", "modifier");
        SnapshotSQLContext actual = updateSnapshotAccessor.getSnapshotSQLContext(executorContext);
        assertThat(actual.getTableName(), is("t_order_0"));
        assertThat(actual.getWhereClause(), is("where t.order_id=? and t.user_id=?"));
        assertThat(actual.getTableAlias(), is("t"));
        List<String> actualColumns = Lists.newArrayList(actual.getQueryColumnNames());
        assertThat(actualColumns, CoreMatchers.<List<String>>is(Lists.newArrayList("status", "modifier", "order_id")));
    }
    
    @Test
    public void assertGetSnapshotSQLContextWithPrimaryKeyColumn() {
        String sql = "update t_order t set t.order_id=?, t.status=?, t.modifier=? where t.order_id=? and t.user_id=?";
        setMockUpdateStatement(sql, "t_order", "t", 60, 93, "order_id", "status", "modifier");
        SnapshotSQLContext actual = updateSnapshotAccessor.getSnapshotSQLContext(executorContext);
        assertThat(actual.getTableName(), is("t_order_0"));
        assertThat(actual.getWhereClause(), is("where t.order_id=? and t.user_id=?"));
        assertThat(actual.getTableAlias(), is("t"));
        List<String> actualColumns = Lists.newArrayList(actual.getQueryColumnNames());
        assertThat(actualColumns, CoreMatchers.<List<String>>is(Lists.newArrayList("order_id", "status", "modifier")));
    }
    
    @Test(expected = IllegalStateException.class)
    public void assertGetSnapshotSQLContextPrimaryKeyNotExist() {
        String sql = "update t_order t set t.order_id=?, t.status=?, t.modifier=? where t.order_id=? and t.user_id=?";
        setMockUpdateStatement(sql, "t_order", "t", 60, 93, "order_id", "status", "modifier");
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Lists.<String>newArrayList());
        updateSnapshotAccessor.getSnapshotSQLContext(executorContext);
    }
    
    @Test
    public void assertQueryUndoData() throws SQLException {
        String sql = "update t_order set status=?, modifier=? where order_id=? and user_id=?";
        setMockUpdateStatement(sql, "t_order", "", 40, 69, "status", "modifier");
        updateSnapshotAccessor.queryUndoData();
        verify(connection).prepareStatement("SELECT status, modifier, order_id FROM t_order_0 where order_id=? and user_id=? ");
    }
    
    @Test
    public void assertQueryUndoDataWithTableAlias() throws SQLException {
        String sql = "update t_order t set t.status=?, t.modifier=? where t.order_id=? and t.user_id=?";
        setMockUpdateStatement(sql, "t_order", "t", 46, 79, "status", "modifier");
        updateSnapshotAccessor.queryUndoData();
        verify(connection).prepareStatement("SELECT status, modifier, order_id FROM t_order_0 t where t.order_id=? and t.user_id=? ");
    }
    
    @Test
    public void assertQueryUndoDataWithPrimaryKeyColumn() throws SQLException {
        String sql = "update t_order t set t.order_id=?, t.status=?, t.modifier=? where t.order_id=? and t.user_id=?";
        setMockUpdateStatement(sql, "t_order", "t", 60, 93, "order_id", "status", "modifier");
        updateSnapshotAccessor.queryUndoData();
        verify(connection).prepareStatement("SELECT order_id, status, modifier FROM t_order_0 t where t.order_id=? and t.user_id=? ");
    }
    
    private void setMockUpdateStatement(final String logicSQL, final String tableName, final String tableAlias, final int whereStartIndex, final int whereStopIndex, final String... updateColumns) {
        when(executorContext.getLogicTableName()).thenReturn(tableName);
        when(executorContext.getLogicSQL()).thenReturn(logicSQL);
        when(updateStatement.getSetAssignment()).thenReturn(mockUpdateAssignments(tableName, updateColumns));
        when(updateStatement.getWhere()).thenReturn(Optional.of(new WhereSegment(whereStartIndex, whereStopIndex, 1)));
        updateStatement.getTables().add(tableSegment);
        when(tableSegment.getTableName()).thenReturn(tableName);
        when(tableSegment.getAlias()).thenReturn(Optional.of(tableAlias));
    }
    
    private SetAssignmentsSegment mockUpdateAssignments(final String tableName, String... columns) {
        Collection<AssignmentSegment> assignments = new LinkedList<>();
        TableSegment tableSegment = new TableSegment(0, 0, tableName);
        for (String each : columns) {
            ColumnSegment columnSegment = new ColumnSegment(0, 0, each);
            columnSegment.setOwner(tableSegment);
            ExpressionSegment expressionSegment = mock(ExpressionSegment.class);
            assignments.add(new AssignmentSegment(0, 0, columnSegment, expressionSegment));
        }
        return new SetAssignmentsSegment(0, 0, assignments);
    }
}