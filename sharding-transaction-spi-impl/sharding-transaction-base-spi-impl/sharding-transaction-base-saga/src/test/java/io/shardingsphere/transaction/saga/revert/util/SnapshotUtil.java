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

package io.shardingsphere.transaction.saga.revert.util;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;

public class SnapshotUtil {
    
    public static final String COLUMN_ORDER_ID = "order_id";
    
    public static final String COLUMN_USER_ID = "user_id";
    
    public static final String COLUMN_STATUS = "status_id";
    
    public static final long ORDER_ID_VALUE = 1;
    
    public static final int USER_ID_VALUE = 2;
    
    public static final String STATUS_VALUE = "test";
    
    private static final String EXPECTED_SQL = "SELECT * FROM t_order_1 WHERE order_id = ?";
    
    private static final String EXPECTED_SQL_WITHOUT_PLACEHOLDER = "SELECT * FROM t_order_1 WHERE order_id = 1";
    
    /**
     * Get snapshot list for unit test.
     *
     * @return snapshots list
     */
    public static List<Map<String, Object>> getSnapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(COLUMN_ORDER_ID, ORDER_ID_VALUE);
        result.put(COLUMN_USER_ID, USER_ID_VALUE);
        result.put(COLUMN_STATUS, STATUS_VALUE);
        return Lists.newArrayList(result);
    }
    
    /**
     * Mock {@code Connection} to get snapshot.
     *
     * @return mock connection
     */
    @SneakyThrows
    public static Connection mockGetSnapshotConnection() {
        Connection result = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(result.prepareStatement(EXPECTED_SQL)).thenReturn(statement);
        when(result.prepareStatement(EXPECTED_SQL_WITHOUT_PLACEHOLDER)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.getObject(1)).thenReturn(ORDER_ID_VALUE);
        when(resultSet.getObject(2)).thenReturn(USER_ID_VALUE);
        when(resultSet.getObject(3)).thenReturn(STATUS_VALUE);
        when(metaData.getColumnCount()).thenReturn(3);
        when(metaData.getColumnName(1)).thenReturn(COLUMN_ORDER_ID);
        when(metaData.getColumnName(2)).thenReturn(COLUMN_USER_ID);
        when(metaData.getColumnName(3)).thenReturn(COLUMN_STATUS);
        return result;
    }
    
    /**
     * Mock {@code TableMetaData} to get snapshot.
     *
     * @return mock table meta data
     */
    public static TableMetaData mockTableMetaData() {
        TableMetaData result = mock(TableMetaData.class);
        Map<String, ColumnMetaData> columnMetaDataMap = new LinkedHashMap<>();
        when(result.getColumns()).thenReturn(columnMetaDataMap);
        columnMetaDataMap.put(COLUMN_ORDER_ID, new ColumnMetaData(COLUMN_ORDER_ID, "BIGINT", true));
        columnMetaDataMap.put(COLUMN_USER_ID, new ColumnMetaData(COLUMN_USER_ID, "INT", false));
        columnMetaDataMap.put(COLUMN_STATUS, new ColumnMetaData(COLUMN_STATUS, "VARCHAR", false));
        return result;
    }
    
    /**
     * Assert right snapshot.
     * 
     * @param snapshot snapshot for assert
     */
    public static void assertSnapshot(final Map<String, Object> snapshot) {
        assertTrue(snapshot.containsKey(COLUMN_ORDER_ID));
        assertThat((long) snapshot.get(COLUMN_ORDER_ID), equalTo(ORDER_ID_VALUE));
        assertTrue(snapshot.containsKey(COLUMN_USER_ID));
        assertThat((int) snapshot.get(COLUMN_USER_ID), equalTo(USER_ID_VALUE));
        assertTrue(snapshot.containsKey(COLUMN_STATUS));
        assertThat(snapshot.get(COLUMN_STATUS).toString(), is(STATUS_VALUE));
    }
    
    /**
     * Assert right snapshot.
     *
     * @param snapshot snapshot for assert
     */
    public static void assertSnapshot(final Iterator snapshot) {
        assertThat((long) snapshot.next(), equalTo(ORDER_ID_VALUE));
        assertThat((int) snapshot.next(), equalTo(USER_ID_VALUE));
        assertThat(snapshot.next().toString(), is(STATUS_VALUE));
    }
}
