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
import io.shardingsphere.transaction.saga.revert.api.DMLSnapshotAccessor;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLExecuteWrapper;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Revert update.
 *
 * @author duhongjun
 */
public final class UpdateRevertSQLExecuteWrapper implements RevertSQLExecuteWrapper<UpdateRevertSQLStatement> {
    
    private DMLSnapshotAccessor snapshotDataAccessor;
    
    private final UpdateStatement updateStatement;
    
    private final List<Object> actualSQLParameters;
    
    private final String actualTableName;
    
    private final Connection connection;
    
    public UpdateRevertSQLExecuteWrapper(final String actualTableName, final UpdateStatement updateStatement, final List<Object> actualSQLParameters,
                                         final TableMetaData tableMetaData, final Connection connection) {
        snapshotDataAccessor = new DMLSnapshotAccessor(new UpdateSnapshotSQLSegment(actualTableName, updateStatement, actualSQLParameters, tableMetaData));
        this.actualTableName = actualTableName;
        this.updateStatement = updateStatement;
        this.actualSQLParameters = actualSQLParameters;
        this.connection = connection;
    }
    
    @Override
    public UpdateRevertSQLStatement createRevertSQLStatement(final List<String> primaryKeyColumns) throws SQLException {
        List<Map<String, Object>> selectSnapshot = snapshotDataAccessor.queryUndoData(connection);
        Map<String, Object> updateColumns = new LinkedHashMap<>();
        for (Entry<Column, SQLExpression> entry : updateStatement.getAssignments().entrySet()) {
            if (entry.getValue() instanceof SQLPlaceholderExpression) {
                updateColumns.put(entry.getKey().getName(), actualSQLParameters.get(((SQLPlaceholderExpression) entry.getValue()).getIndex()));
            } else if (entry.getValue() instanceof SQLTextExpression) {
                updateColumns.put(entry.getKey().getName(), ((SQLTextExpression) entry.getValue()).getText());
            } else if (entry.getValue() instanceof SQLNumberExpression) {
                updateColumns.put(entry.getKey().getName(), ((SQLNumberExpression) entry.getValue()).getNumber());
            }
        }
        return new UpdateRevertSQLStatement(actualTableName, selectSnapshot, updateColumns, primaryKeyColumns, actualSQLParameters);
    }
    
    @Override
    public Optional<RevertSQLUnit> generateRevertSQL(final UpdateRevertSQLStatement revertSQLStatement) {
        if (revertSQLStatement.getSelectSnapshot().isEmpty()) {
            return Optional.absent();
        }
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultKeyword.UPDATE).append(" ");
        builder.append(revertSQLStatement.getActualTable()).append(" ");
        builder.append(DefaultKeyword.SET).append(" ");
        int pos = 0;
        int size = revertSQLStatement.getUpdateColumns().size();
        for (String updateColumn : revertSQLStatement.getUpdateColumns().keySet()) {
            builder.append(updateColumn).append(" = ?");
            if (pos < size - 1) {
                builder.append(",");
            }
            pos++;
        }
        builder.append(" ").append(DefaultKeyword.WHERE).append(" ");
        return Optional.of(fillWhereWithKeys(revertSQLStatement, builder));
    }
    
    private RevertSQLUnit fillWhereWithKeys(final UpdateRevertSQLStatement revertSQLStatement, final StringBuilder builder) {
        int pos = 0;
        for (String key : revertSQLStatement.getKeys()) {
            if (pos > 0) {
                builder.append(" ").append(DefaultKeyword.AND).append(" ");
            }
            builder.append(key).append(" = ? ");
            pos++;
        }
        RevertSQLUnit result = new RevertSQLUnit(builder.toString());
        for (Map<String, Object> each : revertSQLStatement.getSelectSnapshot()) {
            List<Object> eachSQLParams = new LinkedList<>();
            result.getRevertParams().add(eachSQLParams);
            for (String updateColumn : revertSQLStatement.getUpdateColumns().keySet()) {
                eachSQLParams.add(each.get(updateColumn.toLowerCase()));
            }
            for (String key : revertSQLStatement.getKeys()) {
                Object value = revertSQLStatement.getUpdateColumns().get(key);
                if (null != value) {
                    eachSQLParams.add(value);
                } else {
                    eachSQLParams.add(each.get(key));
                }
            }
        }
        return result;
    }
}
