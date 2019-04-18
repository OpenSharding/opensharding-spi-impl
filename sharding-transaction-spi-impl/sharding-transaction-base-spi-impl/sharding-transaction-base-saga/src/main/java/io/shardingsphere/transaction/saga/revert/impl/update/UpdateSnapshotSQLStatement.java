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

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.SnapshotSQLStatement;
import lombok.Getter;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Update snapshot SQL statement.
 *
 * @author zhaojun
 */
public final class UpdateSnapshotSQLStatement implements SnapshotSQLStatement {
    
    @Getter
    private final String actualTableName;
    
    private final UpdateStatement updateStatement;
    
    private final List<Object> actualSQLParameters;
    
    private final List<String> primaryKeys;
    
    public UpdateSnapshotSQLStatement(final String actualTableName, final UpdateStatement updateStatement, final List<Object> actualSQLParameters, final TableMetaData tableMetaData) {
        this.actualTableName = actualTableName;
        this.updateStatement = updateStatement;
        this.actualSQLParameters = actualSQLParameters;
        this.primaryKeys = getPrimaryKeyColumns(tableMetaData);
    }
    
    private List<String> getPrimaryKeyColumns(final TableMetaData tableMetaData) {
        List<String> result = new LinkedList<>();
        for (ColumnMetaData each : tableMetaData.getColumns().values()) {
            if (each.isPrimaryKey()) {
                result.add(each.getColumnName());
            }
        }
        return result;
    }
    
    @Override
    public Collection<String> getQueryColumnNames() {
        Collection<String> result = new LinkedList<>();
        if (primaryKeys.isEmpty()) {
            return Collections.singleton("*");
        }
        List<String> remainPrimaryKeys = new LinkedList<>(primaryKeys);
        for (Column each : updateStatement.getAssignments().keySet()) {
            int dotPosition = each.getName().indexOf('.');
            String columnName = dotPosition > 0 ? each.getName().substring(dotPosition + 1).toLowerCase() : each.getName().toLowerCase();
            result.add(columnName);
            remainPrimaryKeys.remove(columnName);
        }
        result.addAll(remainPrimaryKeys);
        return result;
    }
    
    @Override
    public String getTableAliasLiterals() {
        String result = null;
        Optional<Table> table = updateStatement.getTables().find(updateStatement.getTables().getSingleTableName());
        if (table.isPresent() && table.get().getAlias().isPresent() && !table.get().getAlias().get().equals(table.get().getName())) {
            result = table.get().getAlias().get();
        }
        return result;
    }
    
    @Override
    public String getWhereClauseLiterals() {
        return 0 < updateStatement.getWhereStartIndex() ? updateStatement.getLogicSQL().substring(updateStatement.getWhereStartIndex(), updateStatement.getWhereStopIndex() + 1) : "";
    }
    
    @Override
    public Collection<Object> getQueryParameters() {
        Collection<Object> result = new LinkedList<>();
        for (int i = updateStatement.getWhereParameterStartIndex(); i <= updateStatement.getWhereParameterEndIndex(); i++) {
            result.add(actualSQLParameters.get(i));
        }
        return result;
    }
}
