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

package io.shardingsphere.transaction.saga.revert.execute;

import io.shardingsphere.transaction.saga.revert.execute.insert.InsertRevertSQLContext;
import org.apache.shardingsphere.core.optimize.result.insert.ColumnValueOptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResultUnit;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.rule.DataNode;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InsertRevertSQLContextTest {
    
    @Mock
    private InsertOptimizeResult insertOptimizeResult;
    
    private List<String> primaryKeys = new LinkedList<>();
    
    private String dataSourceName;
    
    private String tableName;
    
    private int shard;
    
    @Before
    public void setUp() {
        dataSourceName = "ds_0";
        tableName = "t_order_0";
        shard = 10;
        when(insertOptimizeResult.getUnits()).thenReturn(mockInsertOptimizeResult("order_id", "user_id", "status"));
    }
    
    private List<InsertOptimizeResultUnit> mockInsertOptimizeResult(final String... columnNames) {
        List<InsertOptimizeResultUnit> result = new LinkedList<>();
        for (int i = 1; i <= shard; i++) {
            InsertOptimizeResultUnit unit = new ColumnValueOptimizeResult(mockColumnNames(columnNames), mockSQLExpression(columnNames.length), mockParameters(columnNames.length));
            unit.getDataNodes().add(new DataNode(dataSourceName, tableName));
            result.add(unit);
        }
        return result;
    }
    
    private SQLExpression[] mockSQLExpression(final int length) {
        SQLExpression[] result = new SQLExpression[length];
        for (int i = 0; i < length; i++) {
            result[i] = mock(SQLExpression.class);
        }
        return result;
    }
    
    private List<String> mockColumnNames(final String... columnNames) {
        List<String> result = new LinkedList<>();
        result.addAll(Arrays.asList(columnNames));
        return result;
    }
    
    private Object[] mockParameters(final int length) {
        Object[] result = new Object[length];
        for (int i = 0; i < length; i++) {
            result[i] = i;
        }
        return result;
    }
    
    @Test
    public void assertCreateInsertRevertSQLContextWithSinglePrimaryKey() {
        primaryKeys.add("user_id");
        InsertRevertSQLContext revertSQLContext = new InsertRevertSQLContext(dataSourceName, tableName, primaryKeys, insertOptimizeResult);
        assertThat(revertSQLContext.getPrimaryKeyInsertValues().size(), is(10));
        for (Map<String, Object> each : revertSQLContext.getPrimaryKeyInsertValues()) {
            assertThat(each.get("user_id"), CoreMatchers.<Object>is(1));
        }
    }
    
    @Test
    public void assertCreateInsertRevertSQLContextWithMultiPrimaryKeys() {
        primaryKeys.add("order_id");
        primaryKeys.add("user_id");
        InsertRevertSQLContext revertSQLContext = new InsertRevertSQLContext(dataSourceName, tableName, primaryKeys, insertOptimizeResult);
        assertThat(revertSQLContext.getPrimaryKeyInsertValues().size(), is(10));
        for (Map<String, Object> each : revertSQLContext.getPrimaryKeyInsertValues()) {
            assertThat(each.get("order_id"), CoreMatchers.<Object>is(0));
            assertThat(each.get("user_id"), CoreMatchers.<Object>is(1));
        }
    }
    
    @Test
    public void assertCreateInsertRevertSQLContextWithoutPrimaryKey() {
        InsertRevertSQLContext revertSQLContext = new InsertRevertSQLContext(dataSourceName, tableName, primaryKeys, insertOptimizeResult);
        assertTrue(revertSQLContext.getPrimaryKeyInsertValues().isEmpty());
    }
}
