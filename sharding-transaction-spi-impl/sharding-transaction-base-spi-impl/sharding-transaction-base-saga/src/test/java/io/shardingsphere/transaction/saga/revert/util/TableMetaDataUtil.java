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
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableMetaDataUtil {
    
    public static final String LOGIC_TABLE_NAME = "t_order";
    
    public static final String ACTUAL_TABLE_NAME = "t_order_1";
    
    public static final String COLUMN_ORDER_ID = "order_id";
    
    public static final String COLUMN_USER_ID = "user_id";
    
    public static final String COLUMN_STATUS = "status";
    
    public static final long ORDER_ID_VALUE = 1;
    
    public static final int USER_ID_VALUE = 2;
    
    public static final String STATUS_VALUE = "test";
    
    public static final List<String> KEYS = Lists.newArrayList("order_id");
    
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
}
