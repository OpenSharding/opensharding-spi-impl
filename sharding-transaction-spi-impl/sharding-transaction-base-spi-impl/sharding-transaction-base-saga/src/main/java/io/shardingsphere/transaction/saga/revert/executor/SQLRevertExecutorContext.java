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

package io.shardingsphere.transaction.saga.revert.executor;

import lombok.Getter;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.optimize.result.OptimizeResult;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.type.RoutingTable;
import org.apache.shardingsphere.core.route.type.TableUnit;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * SQL revert executor context.
 *
 * @author zhaojun
 */
@Getter
public class SQLRevertExecutorContext implements SQLRevertContext {
    
    private SQLStatement sqlStatement;
    
    private OptimizeResult optimizeResult;
    
    private RouteUnit routeUnit;
    
    private Connection connection;
    
    private String dataSourceName;
    
    private String actualTableName;
    
    private List<Object> parameters;
    
    private List<String> primaryKeyColumns;
    
    public SQLRevertExecutorContext(final SQLRouteResult sqlRouteResult, final RouteUnit routeUnit, final TableMetaData tableMetaData, final Connection connection) {
        this.sqlStatement = sqlRouteResult.getSqlStatement();
        this.optimizeResult = sqlRouteResult.getOptimizeResult();
        this.routeUnit = routeUnit;
        this.dataSourceName = routeUnit.getDataSourceName();
        this.actualTableName = getActualTableName(sqlRouteResult.getSqlStatement(), sqlRouteResult.getRoutingResult().getTableUnits().getTableUnits(), routeUnit);
        this.parameters = routeUnit.getSqlUnit().getParameters();
        this.primaryKeyColumns = getPrimaryKeyColumns(tableMetaData);
        this.connection = connection;
    }
    
    private String getActualTableName(final SQLStatement sqlStatement, final Collection<TableUnit> tableUnits, final RouteUnit routeUnit) {
        for (TableUnit each : tableUnits) {
            if (each.getDataSourceName().equalsIgnoreCase(routeUnit.getDataSourceName())) {
                return getAvailableActualTableName(each, sqlStatement.getTables().getSingleTableName());
            }
        }
        throw new ShardingException(String.format("Could not find actual table name of [%s]", routeUnit));
    }
    
    private String getAvailableActualTableName(final TableUnit tableUnit, final String logicTableName) {
        for (RoutingTable each : tableUnit.getRoutingTables()) {
            if (each.getLogicTableName().equalsIgnoreCase(logicTableName)) {
                return each.getActualTableName();
            }
        }
        throw new ShardingException(String.format("Could not get available actual table name of [%s]", tableUnit));
    }
    
    private List<String> getPrimaryKeyColumns(final TableMetaData tableMetaData) {
        List<String> result = new ArrayList<>();
        for (ColumnMetaData each : tableMetaData.getColumns().values()) {
            if (each.isPrimaryKey()) {
                result.add(each.getColumnName());
            }
        }
        if (result.isEmpty()) {
            throw new RuntimeException("Not supported table without primary key");
        }
        return result;
    }
}
