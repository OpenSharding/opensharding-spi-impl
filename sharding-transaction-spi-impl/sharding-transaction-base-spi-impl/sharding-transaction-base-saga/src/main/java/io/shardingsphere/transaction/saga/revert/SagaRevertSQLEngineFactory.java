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

package io.shardingsphere.transaction.saga.revert;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.context.SagaLogicSQLTransaction;
import io.shardingsphere.transaction.saga.revert.engine.DMLRevertSQLRewriteEngine;
import io.shardingsphere.transaction.saga.revert.engine.RevertSQLRewriteEngine;
import io.shardingsphere.transaction.saga.revert.execute.SQLRewriteWrapper;
import io.shardingsphere.transaction.saga.revert.execute.delete.DeleteSQLRewriteWrapper;
import io.shardingsphere.transaction.saga.revert.execute.insert.InsertRevertSQLContext;
import io.shardingsphere.transaction.saga.revert.execute.insert.InsertSQLRewriteWrapper;
import io.shardingsphere.transaction.saga.revert.execute.update.UpdateSQLRewriteWrapper;
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
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.type.RoutingTable;
import org.apache.shardingsphere.core.route.type.TableUnit;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Revert SQL engine factory.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class SagaRevertSQLEngineFactory {
    
    /**
     * Create new revert SQL engine.
     *
     * @param logicSQLTransaction logic SQL transaction
     * @param routeUnit route unit
     * @param connectionMap connection map
     * @return revert SQL engine
     */
    @SneakyThrows
    public static RevertSQLRewriteEngine newInstance(final SagaLogicSQLTransaction logicSQLTransaction, final RouteUnit routeUnit, final ConcurrentMap<String, Connection> connectionMap) {
        SQLStatement sqlStatement = logicSQLTransaction.getSqlRouteResult().getSqlStatement();
        List<Object> parameters = routeUnit.getSqlUnit().getParameters();
        String actualTableName = getActualTableName(logicSQLTransaction.getSqlRouteResult(), routeUnit);
        Connection connection = connectionMap.get(routeUnit.getDataSourceName());
        List<String> primaryKeyColumns = getPrimaryKeyColumns(logicSQLTransaction.getTableMetaData());
        SQLRewriteWrapper sqlRewriteWrapper;
        if (sqlStatement instanceof InsertStatement) {
            Optional<InsertOptimizeResult> insertOptimizeResult = logicSQLTransaction.getSqlRouteResult().getOptimizeResult().getInsertOptimizeResult();
            sqlRewriteWrapper = new InsertSQLRewriteWrapper(new InsertRevertSQLContext(routeUnit.getDataSourceName(), actualTableName, primaryKeyColumns, insertOptimizeResult.orNull()));
        } else if (sqlStatement instanceof DeleteStatement) {
            DeleteSnapshotSQLStatement snapshotSQLStatement = new DeleteSnapshotSQLStatement(actualTableName, (DeleteStatement) sqlStatement, parameters);
            sqlRewriteWrapper = new DeleteSQLRewriteWrapper(new DMLSnapshotAccessor(snapshotSQLStatement, connection));
        } else if (sqlStatement instanceof UpdateStatement) {
            UpdateSnapshotSQLStatement snapshotSQLStatement = new UpdateSnapshotSQLStatement(actualTableName, (UpdateStatement) sqlStatement, parameters, primaryKeyColumns);
            sqlRewriteWrapper = new UpdateSQLRewriteWrapper(new DMLSnapshotAccessor(snapshotSQLStatement, connection));
        } else {
            throw new UnsupportedOperationException("unsupported SQL statement");
        }
        return new DMLRevertSQLRewriteEngine(sqlRewriteWrapper);
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
    
    private static String getActualTableName(final SQLRouteResult sqlRouteResult, final RouteUnit routeUnit) {
        for (TableUnit each : sqlRouteResult.getRoutingResult().getTableUnits().getTableUnits()) {
            if (each.getDataSourceName().equalsIgnoreCase(routeUnit.getDataSourceName())) {
                return getAvailableActualTableName(each, sqlRouteResult.getSqlStatement().getTables().getSingleTableName());
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
