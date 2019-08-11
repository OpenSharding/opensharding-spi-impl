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

package io.shardingsphere.transaction.base.hook.revert.executor;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.base.hook.revert.RevertSQLResult;
import io.shardingsphere.transaction.base.hook.revert.executor.update.UpdateSQLRevertExecutor;
import io.shardingsphere.transaction.base.hook.revert.snapshot.UpdateSnapshotAccessor;
import org.apache.shardingsphere.core.optimize.api.segment.Column;
import org.apache.shardingsphere.core.optimize.api.statement.OptimizedStatement;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.complex.CommonExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.UpdateStatement;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UpdateSQLRevertExecutorTest {
    
    @Mock
    private SQLRevertExecutorContext executorContext;
    
    @Mock
    private UpdateSnapshotAccessor snapshotAccessor;
    
    @Mock
    private OptimizedStatement optimizedStatement;
    
    @Mock
    private UpdateStatement updateStatement;
    
    private RevertSQLResult revertSQLResult = new RevertSQLResult("");
    
    private Map<Column, ExpressionSegment> updateAssignments = new LinkedHashMap<>();
    
    private List<Object> parameters = new LinkedList<>();
    
    private List<Map<String, Object>> undoData = Lists.newLinkedList();
    
    private UpdateSQLRevertExecutor sqlRevertExecutor;
    
    @Before
    public void setUp() throws SQLException {
        when(updateStatement.getAssignments()).thenReturn(updateAssignments);
        when(snapshotAccessor.queryUndoData()).thenReturn(undoData);
        when(executorContext.getOptimizedStatement()).thenReturn(optimizedStatement);
        when(optimizedStatement.getSQLStatement()).thenReturn(updateStatement);
        when(executorContext.getParameters()).thenReturn(parameters);
        when(executorContext.getActualTableName()).thenReturn("t_order_0");
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Lists.newLinkedList(Collections.singleton("order_id")));
    }
    
    private void setUpdateAssignments(final String tableName, String... columns) {
        for (String each : columns) {
            Column column = new Column(each, tableName);
            if ("status".equals(each)) {
                updateAssignments.put(column, new CommonExpressionSegment(0, 0,"modified"));
            } else if ("user_id".equals(each)) {
                updateAssignments.put(column, new LiteralExpressionSegment(0, 0, 1L));
            } else if ("order_id".equals(each)) {
                updateAssignments.put(column, new ParameterMarkerExpressionSegment(0, 0, parameters.size()));
                parameters.add(1000L);
            }
        }
    }
    
    private void setSnapshot(final int count, String... columns) {
        for (int i = 1; i <= count; i++) {
            Map<String, Object> record = new HashMap<>();
            for (String each : columns) {
                record.put(each, each + "_" + i);
            }
            undoData.add(record);
        }
    }
    
    @Test
    public void assertRevertSQLWithSetUpdatePrimaryKey() throws SQLException {
        setUpdateAssignments("t_order", "user_id", "status", "order_id");
        setSnapshot(10, "user_id", "status", "order_id");
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
        Optional<String> actual = sqlRevertExecutor.revertSQL();
        assertTrue(actual.isPresent());
        assertThat(actual.get(), is("UPDATE t_order_0 SET user_id = ?, status = ?, order_id = ? WHERE order_id = ?"));
    }
    
    @Test
    public void assertRevertSQLWithoutSetUpdatePrimaryKey() throws SQLException {
        setUpdateAssignments("t_order", "user_id", "status");
        setSnapshot(10, "user_id", "status", "order_id");
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
        Optional<String> actual = sqlRevertExecutor.revertSQL();
        assertTrue(actual.isPresent());
        assertThat(actual.get(), is("UPDATE t_order_0 SET user_id = ?, status = ? WHERE order_id = ?"));
    }
    
    @Test
    public void assertRevertSQLWithMultiPrimaryKeys() throws SQLException {
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Arrays.asList("order_id", "pk_2"));
        setUpdateAssignments("t_order", "user_id", "status");
        setSnapshot(10, "user_id", "status", "order_id", "pk_2");
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
        Optional<String> actual = sqlRevertExecutor.revertSQL();
        assertTrue(actual.isPresent());
        assertThat(actual.get(), is("UPDATE t_order_0 SET user_id = ?, status = ? WHERE order_id = ? AND pk_2 = ?"));
    }
    
    @Test
    public void assertFillParametersWithSetUpdatePrimaryKey() throws SQLException {
        setUpdateAssignments("t_order", "user_id", "status", "order_id");
        setSnapshot(1, "user_id", "status", "order_id");
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
        sqlRevertExecutor.fillParameters(revertSQLResult);
        assertThat(revertSQLResult.getParameters().size(), is(1));
        for (Collection<Object> each : revertSQLResult.getParameters()) {
            assertThat(each.size(), is(4));
            List<Object> parameterRow = Lists.newArrayList(each);
            assertThat(parameterRow.get(0), CoreMatchers.<Object>is("user_id_1"));
            assertThat(parameterRow.get(1), CoreMatchers.<Object>is("status_1"));
            assertThat(parameterRow.get(2), CoreMatchers.<Object>is("order_id_1"));
            assertThat(parameterRow.get(3), CoreMatchers.<Object>is(1000L));
        }
    }
    
    @Test
    public void assertFillParametersWithoutSetUpdatePrimaryKey() throws SQLException {
        setUpdateAssignments("t_order", "user_id", "status");
        setSnapshot(10, "user_id", "status", "order_id");
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
        sqlRevertExecutor.fillParameters(revertSQLResult);
        assertThat(revertSQLResult.getParameters().size(), is(10));
        int offset = 1;
        for (Collection<Object> each : revertSQLResult.getParameters()) {
            assertThat(each.size(), is(3));
            List<Object> parameterRow = Lists.newArrayList(each);
            assertThat(parameterRow.get(0), CoreMatchers.<Object>is("user_id_" + offset));
            assertThat(parameterRow.get(1), CoreMatchers.<Object>is("status_" + offset));
            assertThat(parameterRow.get(2), CoreMatchers.<Object>is("order_id_" + offset));
            offset++;
        }
    }
    
    @Test
    public void assertFillParametersWithMultiPrimaryKeys() throws SQLException {
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Arrays.asList("order_id", "pk_2"));
        setUpdateAssignments("t_order", "user_id", "status");
        setSnapshot(10, "user_id", "status", "order_id", "pk_2");
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
        sqlRevertExecutor.fillParameters(revertSQLResult);
        assertThat(revertSQLResult.getParameters().size(), is(10));
        int offset = 1;
        for (Collection<Object> each : revertSQLResult.getParameters()) {
            assertThat(each.size(), is(4));
            List<Object> parameterRow = Lists.newArrayList(each);
            assertThat(parameterRow.get(0), CoreMatchers.<Object>is("user_id_" + offset));
            assertThat(parameterRow.get(1), CoreMatchers.<Object>is("status_" + offset));
            assertThat(parameterRow.get(2), CoreMatchers.<Object>is("order_id_" + offset));
            assertThat(parameterRow.get(3), CoreMatchers.<Object>is("pk_2_" + offset));
            offset++;
        }
    }
}