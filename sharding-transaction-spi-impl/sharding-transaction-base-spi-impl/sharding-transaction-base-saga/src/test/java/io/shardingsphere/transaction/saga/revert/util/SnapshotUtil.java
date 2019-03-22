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

public class SnapshotUtil {
    
    /**
     * Get snapshot list for unit test.
     *
     * @return snapshots list
     */
    public static List<Map<String, Object>> getSnapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(TableMetaDataUtil.COLUMN_ORDER_ID, TableMetaDataUtil.ORDER_ID_VALUE);
        result.put(TableMetaDataUtil.COLUMN_USER_ID, TableMetaDataUtil.USER_ID_VALUE);
        result.put(TableMetaDataUtil.COLUMN_STATUS, TableMetaDataUtil.STATUS_VALUE);
        return Lists.newArrayList(result);
    }
    
    /**
     * Mock {@code Connection} to get snapshot.
     *
     * @param exceptedSQL excepted select snapshot SQL
     * @return mock connection
     */
    @SneakyThrows
    public static Connection mockGetSnapshotConnection(final String exceptedSQL) {
        Connection result = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(result.prepareStatement(exceptedSQL)).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.getObject(1)).thenReturn(TableMetaDataUtil.ORDER_ID_VALUE);
        when(resultSet.getObject(2)).thenReturn(TableMetaDataUtil.USER_ID_VALUE);
        when(resultSet.getObject(3)).thenReturn(TableMetaDataUtil.STATUS_VALUE);
        when(metaData.getColumnCount()).thenReturn(3);
        when(metaData.getColumnName(1)).thenReturn(TableMetaDataUtil.COLUMN_ORDER_ID);
        when(metaData.getColumnName(2)).thenReturn(TableMetaDataUtil.COLUMN_USER_ID);
        when(metaData.getColumnName(3)).thenReturn(TableMetaDataUtil.COLUMN_STATUS);
        return result;
    }
    
    /**
     * Assert right snapshot.
     * 
     * @param snapshot snapshot for assert
     */
    public static void assertSnapshot(final Map<String, Object> snapshot) {
        assertTrue(snapshot.containsKey(TableMetaDataUtil.COLUMN_ORDER_ID));
        assertThat((long) snapshot.get(TableMetaDataUtil.COLUMN_ORDER_ID), equalTo(TableMetaDataUtil.ORDER_ID_VALUE));
        assertTrue(snapshot.containsKey(TableMetaDataUtil.COLUMN_USER_ID));
        assertThat((int) snapshot.get(TableMetaDataUtil.COLUMN_USER_ID), equalTo(TableMetaDataUtil.USER_ID_VALUE));
        assertTrue(snapshot.containsKey(TableMetaDataUtil.COLUMN_STATUS));
        assertThat(snapshot.get(TableMetaDataUtil.COLUMN_STATUS).toString(), is(TableMetaDataUtil.STATUS_VALUE));
    }
    
    /**
     * Assert right snapshot.
     *
     * @param snapshot snapshot for assert
     */
    public static void assertSnapshot(final Iterator snapshot) {
        assertThat((long) snapshot.next(), equalTo(TableMetaDataUtil.ORDER_ID_VALUE));
        assertThat((int) snapshot.next(), equalTo(TableMetaDataUtil.USER_ID_VALUE));
        assertThat(snapshot.next().toString(), is(TableMetaDataUtil.STATUS_VALUE));
    }
}
