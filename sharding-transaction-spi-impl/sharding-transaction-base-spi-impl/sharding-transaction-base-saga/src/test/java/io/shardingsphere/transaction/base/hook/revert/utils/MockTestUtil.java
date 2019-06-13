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

package io.shardingsphere.transaction.base.hook.revert.utils;

import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.sql.context.table.Tables;
import org.apache.shardingsphere.core.parse.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.SQLUnit;
import org.apache.shardingsphere.core.route.type.RoutingResult;
import org.apache.shardingsphere.core.route.type.RoutingUnit;
import org.apache.shardingsphere.core.route.type.TableUnit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock test util.
 *
 * @author zhaojun
 */
public class MockTestUtil {
    
    public static SQLRouteResult mockSQLRouteResult(final SQLStatement sqlStatement, final String dataSourceName, final String logicTableName, final String actualTableName) {
        SQLRouteResult result = new SQLRouteResult(sqlStatement);
        result.setRoutingResult(mockRoutingResult(dataSourceName, logicTableName, actualTableName));
        return result;
    }
    
    private static RoutingResult mockRoutingResult(final String dataSourceName, final String logicTableName, final String actualTableName) {
        RoutingResult result = new RoutingResult();
        RoutingUnit tableUnit = mockRoutingUnit(dataSourceName, logicTableName, actualTableName);
        result.getRoutingUnits().add(tableUnit);
        return result;
    }
    
    private static RoutingUnit mockRoutingUnit(final String dataSourceName, final String logicTableName, final String actualTableName) {
        RoutingUnit result = new RoutingUnit(dataSourceName);
        TableUnit tableUnit = new TableUnit(logicTableName, actualTableName);
        result.getTableUnits().add(tableUnit);
        return result;
    }
    
    public static RouteUnit mockRouteUnit(final String dataSourceName, final String actualSQL, final List<Object> parameters) {
        SQLUnit sqlUnit = new SQLUnit(actualSQL, parameters);
        return new RouteUnit(dataSourceName, sqlUnit);
    }
    
    public static DeleteStatement mockDeleteStatement(final String logicTableName) {
        DeleteStatement result = mock(DeleteStatement.class);
        Tables tables = mock(Tables.class);
        when(tables.getSingleTableName()).thenReturn(logicTableName);
        when(result.getTables()).thenReturn(tables);
        return result;
    }
    
    public static TableMetaData mockTableMetaData(final String... columns) {
        Collection<ColumnMetaData> columnMetaDataList = new LinkedList<>();
        for (String each : columns) {
            columnMetaDataList.add(new ColumnMetaData(each, "String", false));
        }
        return new TableMetaData(columnMetaDataList);
    }
    
    public static void addPrimaryKeyColumn(final TableMetaData tableMetaData, final String... columns) {
        for (String each : columns) {
            tableMetaData.getColumns().put(each, new ColumnMetaData(each, "String", true));
        }
    }
    
    public static Connection mockConnection() throws SQLException {
        Connection result = mock(Connection.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(result.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        return result;
    }
}
