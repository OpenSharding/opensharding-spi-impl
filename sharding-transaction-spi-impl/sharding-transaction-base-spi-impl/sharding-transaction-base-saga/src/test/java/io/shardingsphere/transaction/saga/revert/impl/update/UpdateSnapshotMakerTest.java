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
import io.shardingsphere.transaction.saga.revert.util.SnapshotUtil;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.apache.shardingsphere.core.parse.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.parser.sql.dml.DMLStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UpdateSnapshotMakerTest {
    
    private static final String UPDATE_SQL_WITH_KEY = "UPDATE t_order alias SET order_id = ?, user_id = ?, status = ? WHERE order_id = ?";
    
    private static final String EXCEPTED_SQL_WITH_KEY = "SELECT order_id, user_id, status FROM t_order_1 alias  WHERE order_id = ?";
    
    private static final List<Object> ACTUAL_PARAMS_WITH_KEY = Lists.<Object>newArrayList(1, 2, "xxx", 1);
    
    private static final String UPDATE_SQL_WITHOUT_KEY = "UPDATE t_order alias SET user_id = ?, status = ? WHERE order_id = ?";
    
    private static final String EXCEPTED_SQL_WITHOUT_KEY = "SELECT user_id, status, order_id FROM t_order_1 alias   WHERE order_id = ?";
    
    private static final List<Object> ACTUAL_PARAMS_WITHOUT_KEY = Lists.<Object>newArrayList(2, "xxx", 1);
    
    private static final String EXCEPTED_SQL = "SELECT * FROM t_order_1 alias   WHERE order_id = ?";
    
    @Mock
    private DMLStatement dmlStatementWithKey;
    
    @Mock
    private DMLStatement dmlStatementWithoutKey;
    
    @Test
    public void assertMakeWithKey() throws SQLException {
        mockStatementWithKey();
        SnapshotParameter snapshotParameter = new SnapshotParameter(null, dmlStatementWithKey, SnapshotUtil.mockGetSnapshotConnection(EXCEPTED_SQL_WITH_KEY),
            TableMetaDataUtil.ACTUAL_TABLE_NAME, UPDATE_SQL_WITH_KEY, null, ACTUAL_PARAMS_WITH_KEY);
        UpdateSnapshotMaker snapshotMaker = new UpdateSnapshotMaker();
        List<Map<String, Object>> snapshots = snapshotMaker.make(snapshotParameter, TableMetaDataUtil.KEYS);
        assertThat(snapshots.size(), is(1));
        SnapshotUtil.assertSnapshot(snapshots.get(0));
    }
    
    private void mockStatementWithKey() {
        when(dmlStatementWithKey.getWhereStartIndex()).thenReturn(63);
        when(dmlStatementWithKey.getWhereStopIndex()).thenReturn(80);
        Map<String, String> alias = new HashMap<>();
        alias.put("alias", "t_order");
        when(dmlStatementWithKey.getUpdateTableAlias()).thenReturn(alias);
        Map<Column, SQLExpression> columnValues = new LinkedHashMap<>();
        columnValues.put(new Column(TableMetaDataUtil.COLUMN_ORDER_ID, "t_order"), new SQLPlaceholderExpression(0));
        columnValues.put(new Column(TableMetaDataUtil.COLUMN_USER_ID, "t_order"), new SQLPlaceholderExpression(1));
        columnValues.put(new Column(TableMetaDataUtil.COLUMN_STATUS, "t_order"), new SQLPlaceholderExpression(2));
        when(dmlStatementWithKey.getUpdateColumnValues()).thenReturn(columnValues);
    }
    
    @Test
    public void assertMakeWithoutKey() throws SQLException {
        mockStatementWithoutKey();
        SnapshotParameter snapshotParameter = new SnapshotParameter(null, dmlStatementWithoutKey, SnapshotUtil.mockGetSnapshotConnection(EXCEPTED_SQL_WITHOUT_KEY),
            TableMetaDataUtil.ACTUAL_TABLE_NAME, UPDATE_SQL_WITHOUT_KEY, null, ACTUAL_PARAMS_WITHOUT_KEY);
        UpdateSnapshotMaker snapshotMaker = new UpdateSnapshotMaker();
        List<Map<String, Object>> snapshots = snapshotMaker.make(snapshotParameter, TableMetaDataUtil.KEYS);
        assertThat(snapshots.size(), is(1));
        SnapshotUtil.assertSnapshot(snapshots.get(0));
    }
    
    private void mockStatementWithoutKey() {
        when(dmlStatementWithoutKey.getWhereStartIndex()).thenReturn(48);
        when(dmlStatementWithoutKey.getWhereStopIndex()).thenReturn(66);
        Map<String, String> alias = new HashMap<>();
        alias.put("alias", "t_order");
        when(dmlStatementWithoutKey.getUpdateTableAlias()).thenReturn(alias);
        Map<Column, SQLExpression> columnValues = new LinkedHashMap<>();
        columnValues.put(new Column(TableMetaDataUtil.COLUMN_USER_ID, "t_order"), new SQLPlaceholderExpression(0));
        columnValues.put(new Column(TableMetaDataUtil.COLUMN_STATUS, "t_order"), new SQLPlaceholderExpression(1));
        when(dmlStatementWithoutKey.getUpdateColumnValues()).thenReturn(columnValues);
    }
    
    @Test
    public void assertMakeNoKey() throws SQLException {
        mockStatementWithoutKey();
        SnapshotParameter snapshotParameter = new SnapshotParameter(null, dmlStatementWithoutKey, SnapshotUtil.mockGetSnapshotConnection(EXCEPTED_SQL),
            TableMetaDataUtil.ACTUAL_TABLE_NAME, UPDATE_SQL_WITHOUT_KEY, null, ACTUAL_PARAMS_WITHOUT_KEY);
        UpdateSnapshotMaker snapshotMaker = new UpdateSnapshotMaker();
        List<Map<String, Object>> snapshots = snapshotMaker.make(snapshotParameter, Lists.<String>newArrayList());
        assertThat(snapshots.size(), is(1));
        SnapshotUtil.assertSnapshot(snapshots.get(0));
    }
}
