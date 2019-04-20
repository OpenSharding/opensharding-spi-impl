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

import io.shardingsphere.transaction.saga.revert.executor.delete.DeleteSQLRevertExecutor;
import io.shardingsphere.transaction.saga.revert.executor.insert.InsertSQLRevertContext;
import io.shardingsphere.transaction.saga.revert.executor.insert.InsertSQLRevertExecutor;
import io.shardingsphere.transaction.saga.revert.executor.update.UpdateSQLRevertExecutor;
import io.shardingsphere.transaction.saga.revert.snapshot.DMLSnapshotAccessor;
import io.shardingsphere.transaction.saga.revert.snapshot.statement.DeleteSnapshotSQLStatement;
import io.shardingsphere.transaction.saga.revert.snapshot.statement.UpdateSnapshotSQLStatement;
import lombok.SneakyThrows;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.type.RoutingTable;
import org.apache.shardingsphere.core.route.type.TableUnit;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * SQL revert executor factory.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class SQLRevertExecutorFactory {
    
    /**
     * Create new revert SQL executor.
     *
     * @param routeUnit route unit
     * @return revert SQL engine
     */
    @SneakyThrows
    public static SQLRevertExecutor newInstance(final SQLStatement sqlStatement, final Collection<TableUnit> tableUnits, InsertOptimizeResult insertOptimizeResult,
                                                final RouteUnit routeUnit, final TableMetaData tableMetaData, final Connection connection) {
        List<Object> parameters = routeUnit.getSqlUnit().getParameters();
        String actualTableName = getActualTableName(sqlStatement, tableUnits, routeUnit);
        List<String> primaryKeyColumns = getPrimaryKeyColumns(tableMetaData);
        SQLRevertExecutor sqlRevertExecutor;
        if (sqlStatement instanceof InsertStatement) {
            sqlRevertExecutor = new InsertSQLRevertExecutor(new InsertSQLRevertContext(routeUnit.getDataSourceName(), actualTableName, primaryKeyColumns, insertOptimizeResult));
        } else if (sqlStatement instanceof DeleteStatement) {
            DeleteSnapshotSQLStatement snapshotSQLStatement = new DeleteSnapshotSQLStatement(actualTableName, (DeleteStatement) sqlStatement, parameters);
            sqlRevertExecutor = new DeleteSQLRevertExecutor(new DMLSnapshotAccessor(snapshotSQLStatement, connection));
        } else if (sqlStatement instanceof UpdateStatement) {
            UpdateSnapshotSQLStatement snapshotSQLStatement = new UpdateSnapshotSQLStatement(actualTableName, (UpdateStatement) sqlStatement, parameters, primaryKeyColumns);
            sqlRevertExecutor = new UpdateSQLRevertExecutor(new DMLSnapshotAccessor(snapshotSQLStatement, connection));
        } else {
            throw new UnsupportedOperationException("unsupported SQL statement");
        }
        return sqlRevertExecutor;
    }
    
    private static List<String> getPrimaryKeyColumns(final TableMetaData tableMetaData) {
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
    
    private static String getActualTableName(final SQLStatement sqlStatement, final Collection<TableUnit> tableUnits, final RouteUnit routeUnit) {
        for (TableUnit each : tableUnits) {
            if (each.getDataSourceName().equalsIgnoreCase(routeUnit.getDataSourceName())) {
                return getAvailableActualTableName(each, sqlStatement.getTables().getSingleTableName());
            }
        }
        throw new ShardingException(String.format("Could not find actual table name of [%s]", routeUnit));
    }
    
    private static String getAvailableActualTableName(final TableUnit tableUnit, final String logicTableName) {
        for (RoutingTable each : tableUnit.getRoutingTables()) {
            if (each.getLogicTableName().equalsIgnoreCase(logicTableName)) {
                return each.getActualTableName();
            }
        }
        throw new ShardingException(String.format("Could not get available actual table name of [%s]", tableUnit));
    }
}
