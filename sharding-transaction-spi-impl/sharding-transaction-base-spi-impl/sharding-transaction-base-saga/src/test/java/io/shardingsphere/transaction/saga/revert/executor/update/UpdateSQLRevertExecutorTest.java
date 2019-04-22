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

package io.shardingsphere.transaction.saga.revert.executor.update;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.executor.SQLRevertExecutorContext;
import io.shardingsphere.transaction.saga.revert.snapshot.UpdateSnapshotAccessor;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
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
    private UpdateStatement updateStatement;
    
    private Map<Column, SQLExpression> updateAssignments = new LinkedHashMap<>();
    
    private List<Object> parameters = new LinkedList<>();
    
    private List<Map<String, Object>> undoData = Lists.newLinkedList();
    
    private UpdateSQLRevertExecutor sqlRevertExecutor;
    
    @Before
    public void setUp() throws SQLException {
        addUpdateAssignment("t_order", "status", new SQLTextExpression("init"));
        addUpdateAssignment("t_order", "user_id", new SQLNumberExpression(22L));
        addUpdateAssignment("t_order", "order_id", new SQLPlaceholderExpression(parameters.size()));
        parameters.add(12345L);
        addUndoData(10, "user_id", "status");
        when(updateStatement.getAssignments()).thenReturn(updateAssignments);
        when(snapshotAccessor.queryUndoData()).thenReturn(undoData);
        when(executorContext.getSqlStatement()).thenReturn(updateStatement);
        when(executorContext.getParameters()).thenReturn(parameters);
        when(executorContext.getActualTableName()).thenReturn("t_order_0");
        when(executorContext.getPrimaryKeyColumns()).thenReturn(Lists.newLinkedList(Collections.singleton("order_id")));
        sqlRevertExecutor = new UpdateSQLRevertExecutor(executorContext, snapshotAccessor);
    }
    
    private void addUndoData(final int count, String... columns) {
        for (int i = 1; i <= count; i++) {
            Map<String, Object> record = new HashMap<>();
            for (String each : columns) {
                record.put(each, i);
            }
            undoData.add(record);
        }
    }
    
    private void addUpdateAssignment(final String tableName, final String columnName, final SQLExpression sqlExpression) {
        Column column = new Column(columnName, tableName);
        updateAssignments.put(column, sqlExpression);
    }
    
    @Test
    public void revertSQL() {
        Optional<String> actual = sqlRevertExecutor.revertSQL();
        assertTrue(actual.isPresent());
        assertThat(actual.get(), is("UPDATE t_order_0 SET status = ?, user_id = ?, order_id = ? WHERE order_id = ?"));
        
    }
    
    @Test
    public void fillParameters() {
    }
    
    
}