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

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.apache.shardingsphere.core.parse.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.parser.context.insertvalue.InsertValue;
import org.apache.shardingsphere.core.parse.parser.context.insertvalue.InsertValues;
import org.apache.shardingsphere.core.parse.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.parser.expression.SQLTextExpression;
import org.apache.shardingsphere.core.parse.parser.sql.dml.insert.InsertStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RevertInsertTest {
    
    @Mock
    private RevertInsertGenerator revertInsertGenerator;
    
    @Mock
    private InsertStatement insertStatement;
    
    private SnapshotParameter snapshotParameter;
    
    @Before
    public void setUp() throws Exception {
        snapshotParameter = new SnapshotParameter(TableMetaDataUtil.mockTableMetaData(), insertStatement, null,
            TableMetaDataUtil.ACTUAL_TABLE_NAME, null, null, Lists.<Object>newArrayList(TableMetaDataUtil.ORDER_ID_VALUE));
    }
    
    private void mockInsertStatement() {
        when(insertStatement.isContainGenerateKey()).thenReturn(true);
        List<Column> columns = Lists.newArrayList();
        InsertValue insertValue = new InsertValue(DefaultKeyword.VALUES, 0);
        InsertValues insertValues = new InsertValues();
        insertValues.getInsertValues().add(insertValue);
        columns.add(new Column(TableMetaDataUtil.COLUMN_ORDER_ID, TableMetaDataUtil.LOGIC_TABLE_NAME));
        insertValue.getColumnValues().add(new SQLPlaceholderExpression(0));
        columns.add(new Column(TableMetaDataUtil.COLUMN_ORDER_ID, TableMetaDataUtil.LOGIC_TABLE_NAME));
        insertValue.getColumnValues().add(new SQLNumberExpression(2));
        columns.add(new Column(TableMetaDataUtil.COLUMN_ORDER_ID, TableMetaDataUtil.LOGIC_TABLE_NAME));
        insertValue.getColumnValues().add(new SQLTextExpression("test"));
        columns.add(new Column(TableMetaDataUtil.COLUMN_USER_ID, TableMetaDataUtil.LOGIC_TABLE_NAME));
        insertValue.getColumnValues().add(new SQLNumberExpression(2));
        columns.add(new Column(TableMetaDataUtil.COLUMN_STATUS, TableMetaDataUtil.LOGIC_TABLE_NAME));
        insertValue.getColumnValues().add(new SQLTextExpression("test"));
        when(insertStatement.getColumns()).thenReturn(columns);
        when(insertStatement.getInsertValues()).thenReturn(insertValues);
    }
    
    @Test
    public void assertSnapshot() throws SQLException {
        mockInsertStatement();
        RevertInsert revertInsert = new RevertInsert();
        revertInsert.setRevertSQLGenerator(revertInsertGenerator);
        revertInsert.snapshot(snapshotParameter);
        verify(revertInsertGenerator).generate(any(RevertInsertGeneratorParameter.class));
    }
}
