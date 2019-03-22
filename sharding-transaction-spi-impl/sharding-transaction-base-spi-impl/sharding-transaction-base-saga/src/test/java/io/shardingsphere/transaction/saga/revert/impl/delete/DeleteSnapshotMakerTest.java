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

package io.shardingsphere.transaction.saga.revert.impl.delete;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.util.SnapshotUtil;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;

import org.apache.shardingsphere.core.parse.parser.sql.dml.DMLStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeleteSnapshotMakerTest {
    
    private static final String LOGIC_SQL = "DELETE FROM t_order WHERE order_id = ?";
    
    private static final String LOGIC_SQL_WITHOUT_PLACEHOLDER = "DELETE FROM t_order WHERE order_id = 1";
    
    private static final String EXPECTED_SQL = "SELECT * FROM t_order_1 WHERE order_id = ?";
    
    private static final String EXPECTED_SQL_WITHOUT_PLACEHOLDER = "SELECT * FROM t_order_1 WHERE order_id = 1";
    
    private static final List<Object> ACTUAL_PARAMS = Lists.<Object>newArrayList(1);

    @Mock
    private DMLStatement dmlStatement;
    
    @Before
    public void setUp() throws Exception {
        mockStatement();
    }
    
    private void mockStatement() {
        when(dmlStatement.getWhereStartIndex()).thenReturn(20);
        when(dmlStatement.getWhereStopIndex()).thenReturn(37);
    }
    
    @Test
    public void assertMake() throws SQLException {
        SnapshotParameter snapshotParameter = new SnapshotParameter(null, dmlStatement, SnapshotUtil.mockGetSnapshotConnection(EXPECTED_SQL),
            TableMetaDataUtil.ACTUAL_TABLE_NAME, LOGIC_SQL, null, ACTUAL_PARAMS);
        DeleteSnapshotMaker maker = new DeleteSnapshotMaker();
        List<Map<String, Object>> snapshots = maker.make(snapshotParameter, TableMetaDataUtil.KEYS);
        assertThat(snapshots.size(), is(1));
        SnapshotUtil.assertSnapshot(snapshots.get(0));
    }
    
    @Test
    public void assertMakeEmptyActualParams() throws SQLException {
        SnapshotParameter snapshotParameter = new SnapshotParameter(null, dmlStatement, SnapshotUtil.mockGetSnapshotConnection(EXPECTED_SQL_WITHOUT_PLACEHOLDER),
            TableMetaDataUtil.ACTUAL_TABLE_NAME, LOGIC_SQL_WITHOUT_PLACEHOLDER, null, null);
        DeleteSnapshotMaker maker = new DeleteSnapshotMaker();
        List<Map<String, Object>> snapshots = maker.make(snapshotParameter, TableMetaDataUtil.KEYS);
        assertThat(snapshots.size(), is(1));
        SnapshotUtil.assertSnapshot(snapshots.get(0));
    }
}
