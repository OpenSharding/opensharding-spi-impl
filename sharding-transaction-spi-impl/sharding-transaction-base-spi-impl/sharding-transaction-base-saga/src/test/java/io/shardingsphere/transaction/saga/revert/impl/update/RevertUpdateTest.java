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

package io.shardingsphere.transaction.saga.revert.impl.update;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RevertUpdateTest {
    
    @Mock
    private RevertUpdateGenerator revertUpdateGenerator;
    
    @Mock
    private UpdateSnapshotMaker snapshotMaker;
    
    @Mock
    private UpdateStatement updateStatement;
    
    private SnapshotParameter snapshotParameter;
    
    @Before
    public void setUp() {
        Map<Column, SQLExpression> assignments = new LinkedHashMap<>();
        assignments.put(new Column(TableMetaDataUtil.COLUMN_ORDER_ID, "t_order"), new SQLPlaceholderExpression(0));
        assignments.put(new Column(TableMetaDataUtil.COLUMN_USER_ID, "t_order"), new SQLNumberExpression(2));
        assignments.put(new Column(TableMetaDataUtil.COLUMN_STATUS, "t_order"), new SQLTextExpression(TableMetaDataUtil.STATUS_VALUE));
        when(updateStatement.getAssignments()).thenReturn(assignments);
        snapshotParameter = new SnapshotParameter(TableMetaDataUtil.mockTableMetaData(), updateStatement, null, TableMetaDataUtil.ACTUAL_TABLE_NAME, null, null, Lists.<Object>newArrayList(1L));
    }
    
//    @Test
//    public void assertSnapshot() throws Exception {
//        RevertUpdate revertUpdate = new RevertUpdate();
//        revertUpdate.setSnapshotMaker(snapshotMaker);
//        revertUpdate.setRevertSQLGenerator(revertUpdateGenerator);
//        revertUpdate.snapshot(snapshotParameter);
//        verify(snapshotMaker).make(eq(snapshotParameter), ArgumentMatchers.<String>anyList());
//        verify(revertUpdateGenerator).generate(any(RevertUpdateGeneratorParameter.class));
//    }
}
