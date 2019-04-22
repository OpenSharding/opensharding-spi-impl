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

package io.shardingsphere.transaction.saga.revert.executor;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.RevertSQLResult;
import io.shardingsphere.transaction.saga.revert.executor.insert.InsertSQLRevertContext;
import io.shardingsphere.transaction.saga.revert.executor.insert.InsertSQLRevertExecutor;
import lombok.SneakyThrows;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InsertSQLRevertExecutorTest {
    
    @Mock
    private InsertSQLRevertContext revertSQLContext;
    
    private InsertSQLRevertExecutor insertSQLRevertExecutor;
    
    private RevertSQLResult revertSQLResult = new RevertSQLResult("");
    
    @Before
    @SneakyThrows
    public void setUp() {
        when(revertSQLContext.getActualTable()).thenReturn("t_order_0");
        insertSQLRevertExecutor = new InsertSQLRevertExecutor(revertSQLContext);
    }
    
    private Collection<Map<String, Object>> mockPrimaryKeyInsertValues(final int count, final String... primaryKeys) {
        Collection<Map<String, Object>> result = new LinkedList<>();
        for (int i = 1; i <= count; i++) {
            Map<String, Object> primaryKeyInsertValue = new LinkedHashMap<>();
            for (String each : primaryKeys) {
                primaryKeyInsertValue.put(each, each + "_" + i);
            }
            if (!primaryKeyInsertValue.isEmpty()) {
                result.add(primaryKeyInsertValue);
            }
        }
        return result;
    }
    
    @Test
    public void assertGenerateRevertSQLWithMultiPrimaryKeys() {
        when(revertSQLContext.getPrimaryKeyInsertValues()).thenReturn(mockPrimaryKeyInsertValues(10, "user_id", "order_id"));
        Optional<String> actual = insertSQLRevertExecutor.revertSQL();
        assertTrue(actual.isPresent());
        assertThat(actual.get(), is("DELETE FROM t_order_0 WHERE user_id = ? AND order_id = ?"));
    }
    
    @Test
    public void assertFillParametersWithMultiPrimaryKeys() {
        when(revertSQLContext.getPrimaryKeyInsertValues()).thenReturn(mockPrimaryKeyInsertValues(10, "user_id", "order_id"));
        insertSQLRevertExecutor.fillParameters(revertSQLResult);
        assertThat(revertSQLResult.getParameters().size(), is(10));
        Collection<Object> firstItem = revertSQLResult.getParameters().iterator().next();
        assertThat(firstItem.size(), is(2));
        Iterator iterator = firstItem.iterator();
        assertThat(iterator.next(), CoreMatchers.<Object>is("user_id_1"));
        assertThat(iterator.next(), CoreMatchers.<Object>is("order_id_1"));
    }
    
    @Test
    public void assertGenerateSQLWithSinglePrimaryKey() {
        when(revertSQLContext.getPrimaryKeyInsertValues()).thenReturn(mockPrimaryKeyInsertValues(5, "user_id"));
        Optional<String> actual = insertSQLRevertExecutor.revertSQL();
        assertTrue(actual.isPresent());
        assertThat(actual.get(), is("DELETE FROM t_order_0 WHERE user_id = ?"));
    }
    
    @Test
    public void assertFillParametersWithSinglePrimaryKey() {
        when(revertSQLContext.getPrimaryKeyInsertValues()).thenReturn(mockPrimaryKeyInsertValues(5, "user_id"));
        insertSQLRevertExecutor.fillParameters(revertSQLResult);
        assertThat(revertSQLResult.getParameters().size(), is(5));
        Collection<Object> firstItem = revertSQLResult.getParameters().iterator().next();
        assertThat(firstItem.size(), is(1));
        Iterator iterator = firstItem.iterator();
        assertThat(iterator.next(), CoreMatchers.<Object>is("user_id_1"));
    }
    
    @Test(expected = IllegalStateException.class)
    public void assertGenerateSQLWithoutPrimaryKeyValue() {
        when(revertSQLContext.getPrimaryKeyInsertValues()).thenReturn(mockPrimaryKeyInsertValues(5));
        insertSQLRevertExecutor.revertSQL();
    }
}
