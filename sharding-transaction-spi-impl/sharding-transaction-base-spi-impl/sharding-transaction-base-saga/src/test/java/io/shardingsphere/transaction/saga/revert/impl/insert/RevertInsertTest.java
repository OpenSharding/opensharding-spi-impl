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
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.insertvalue.InsertValue;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RevertInsertTest {
    
    @Mock
    private RevertInsertSQLGenerator revertInsertGenerator;
    
    @Mock
    private InsertStatement insertStatement;
    
    private SnapshotParameter snapshotParameter;
    
    @Before
    public void setUp() {
        snapshotParameter = new SnapshotParameter(TableMetaDataUtil.mockTableMetaData(), insertStatement, null,
            TableMetaDataUtil.ACTUAL_TABLE_NAME, null, null, Lists.<Object>newArrayList(TableMetaDataUtil.ORDER_ID_VALUE));
    }
    
    @SuppressWarnings("unchecked")
    private void mockInsertStatement() {
        List<String> columnNames = Lists.newArrayList();
        columnNames.add(TableMetaDataUtil.COLUMN_ORDER_ID);
        columnNames.add(TableMetaDataUtil.COLUMN_USER_ID);
        columnNames.add(TableMetaDataUtil.COLUMN_STATUS);
        when(insertStatement.getColumnNames()).thenReturn(columnNames);
        Collection<SQLExpression> assignments = Lists.newArrayList();
        assignments.add(new SQLPlaceholderExpression(0));
        assignments.add(new SQLTextExpression("test"));
        assignments.add(new SQLTextExpression("test"));
        InsertValue insertValue = new InsertValue(assignments);
        List<InsertValue> insertValues = Lists.newArrayList();
        insertValues.add(insertValue);
        when(insertStatement.getValues()).thenReturn(insertValues);
    }
    
    @Test
    public void assertSnapshot() throws SQLException {
        mockInsertStatement();
        InsertRevertSQLExecuteWrapper revertInsert = new InsertRevertSQLExecuteWrapper(actualTable, insertStatement, actualSQLParameters);
        revertInsert.setRevertSQLGenerator(revertInsertGenerator);
        revertInsert.snapshot(snapshotParameter);
        verify(revertInsertGenerator).generate(any(InsertRevertSQLStatement.class));
    }
}
