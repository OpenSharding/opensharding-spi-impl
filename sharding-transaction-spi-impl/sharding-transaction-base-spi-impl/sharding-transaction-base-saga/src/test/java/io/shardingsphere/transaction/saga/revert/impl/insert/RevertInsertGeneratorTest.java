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

package io.shardingsphere.transaction.saga.revert.impl.insert;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.apache.shardingsphere.core.parse.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.parser.context.insertvalue.InsertValue;
import org.apache.shardingsphere.core.parse.parser.context.insertvalue.InsertValues;
import org.apache.shardingsphere.core.parse.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.parser.expression.SQLTextExpression;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RevertInsertGeneratorTest {
    
    private RevertInsertGeneratorParameter insertGeneratorParameter;
    
    @Test
    public void generateWithKeyGenerator() throws Exception {
        mockInsertStatementWithKeyGenerator();
        RevertInsertGenerator insertGenerator = new RevertInsertGenerator();
        Optional<RevertContext> revertContext = insertGenerator.generate(insertGeneratorParameter);
        assertTrue(revertContext.isPresent());
        assertThat(revertContext.get().getRevertSQL(), is("DELETE FROM t_order_1 WHERE order_id = ?"));
        assertThat(revertContext.get().getRevertParams().size(), is(1));
        assertThat(revertContext.get().getRevertParams().get(0).size(), is(1));
        Iterator iterator = revertContext.get().getRevertParams().get(0).iterator();
        assertThat((long) iterator.next(), equalTo(TableMetaDataUtil.ORDER_ID_VALUE));
    }
    
    private void mockInsertStatementWithKeyGenerator() {
        List<String> columnNames = Lists.newArrayList();
        InsertValue insertValue = new InsertValue(DefaultKeyword.VALUES, 0);
        InsertValues insertValues = new InsertValues();
        columnNames.add(TableMetaDataUtil.COLUMN_USER_ID);
        insertValue.getColumnValues().add(new SQLNumberExpression(TableMetaDataUtil.USER_ID_VALUE));
        columnNames.add(TableMetaDataUtil.COLUMN_STATUS);
        insertValue.getColumnValues().add(new SQLTextExpression(TableMetaDataUtil.STATUS_VALUE));
        insertValues.getInsertValues().add(insertValue);
        insertGeneratorParameter = new RevertInsertGeneratorParameter(TableMetaDataUtil.ACTUAL_TABLE_NAME, columnNames,
            TableMetaDataUtil.KEYS, Lists.<Object>newArrayList(TableMetaDataUtil.USER_ID_VALUE, TableMetaDataUtil.STATUS_VALUE, TableMetaDataUtil.ORDER_ID_VALUE), 1, true);
    }
    
    @Test
    public void generateWithoutKeyGenerator() throws Exception {
        mockInsertStatementWithoutKeyGenerator();
        RevertInsertGenerator insertGenerator = new RevertInsertGenerator();
        Optional<RevertContext> revertContext = insertGenerator.generate(insertGeneratorParameter);
        assertTrue(revertContext.isPresent());
        assertThat(revertContext.get().getRevertSQL(), is("DELETE FROM t_order_1 WHERE order_id = ?"));
        assertThat(revertContext.get().getRevertParams().size(), is(1));
        assertThat(revertContext.get().getRevertParams().get(0).size(), is(1));
        Iterator iterator = revertContext.get().getRevertParams().get(0).iterator();
        assertThat((long) iterator.next(), equalTo(1L));
    }
    
    private void mockInsertStatementWithoutKeyGenerator() {
        Map<String, Object> keyValue = new HashMap<>();
        List<String> columnNames = Lists.newArrayList();
        InsertValue insertValue = new InsertValue(DefaultKeyword.VALUES, 0);
        InsertValues insertValues = new InsertValues();
        columnNames.add(TableMetaDataUtil.COLUMN_ORDER_ID);
        keyValue.put(TableMetaDataUtil.COLUMN_ORDER_ID, 1L);
        insertValue.getColumnValues().add(new SQLPlaceholderExpression(0));
        columnNames.add(TableMetaDataUtil.COLUMN_USER_ID);
        insertValue.getColumnValues().add(new SQLNumberExpression(TableMetaDataUtil.USER_ID_VALUE));
        columnNames.add(TableMetaDataUtil.COLUMN_STATUS);
        insertValue.getColumnValues().add(new SQLTextExpression(TableMetaDataUtil.STATUS_VALUE));
        insertValues.getInsertValues().add(insertValue);
        insertGeneratorParameter = new RevertInsertGeneratorParameter(TableMetaDataUtil.ACTUAL_TABLE_NAME, columnNames,
            TableMetaDataUtil.KEYS, Lists.<Object>newArrayList(TableMetaDataUtil.ORDER_ID_VALUE, TableMetaDataUtil.USER_ID_VALUE, TableMetaDataUtil.STATUS_VALUE), 1, false);
        insertGeneratorParameter.getKeyValues().add(keyValue);
    }
}
