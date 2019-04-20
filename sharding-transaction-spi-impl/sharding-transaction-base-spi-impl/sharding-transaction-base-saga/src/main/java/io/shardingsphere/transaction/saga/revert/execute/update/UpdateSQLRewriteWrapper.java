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

package io.shardingsphere.transaction.saga.revert.execute.update;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.execute.SQLRewriteWrapper;
import io.shardingsphere.transaction.saga.revert.snapshot.DMLSnapshotAccessor;
import io.shardingsphere.transaction.saga.revert.snapshot.GenericSQLBuilder;
import io.shardingsphere.transaction.saga.revert.snapshot.statement.UpdateSnapshotSQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Update SQL rewrite wrapper.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class UpdateSQLRewriteWrapper implements SQLRewriteWrapper {
    
    private UpdateSQLRevertContext sqlRevertContext;
    
    private final GenericSQLBuilder sqlBuilder = new GenericSQLBuilder();
    
    public UpdateSQLRewriteWrapper(final DMLSnapshotAccessor snapshotAccessor) throws SQLException {
        sqlRevertContext = createRevertSQLContext(snapshotAccessor);
    }
    
    private UpdateSQLRevertContext createRevertSQLContext(final DMLSnapshotAccessor snapshotAccessor) throws SQLException {
        UpdateSnapshotSQLStatement snapshotSQLStatement = (UpdateSnapshotSQLStatement) snapshotAccessor.getSnapshotSQLStatement();
        UpdateStatement updateStatement = snapshotSQLStatement.getUpdateStatement();
        List<Object> parameters = (List<Object>) snapshotSQLStatement.getParameters();
        Map<String, Object> updateSetAssignments = getUpdateSetAssignments(updateStatement, parameters);
        return new UpdateSQLRevertContext(snapshotSQLStatement.getTableName(), snapshotAccessor.queryUndoData(), updateSetAssignments, snapshotSQLStatement.getPrimaryKeyColumns(), parameters);
    }
    
    private Map<String, Object> getUpdateSetAssignments(final UpdateStatement updateStatement, final List<Object> parameters) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Entry<Column, SQLExpression> entry : updateStatement.getAssignments().entrySet()) {
            if (entry.getValue() instanceof SQLPlaceholderExpression) {
                result.put(entry.getKey().getName(), parameters.get(((SQLPlaceholderExpression) entry.getValue()).getIndex()));
            } else if (entry.getValue() instanceof SQLTextExpression) {
                result.put(entry.getKey().getName(), ((SQLTextExpression) entry.getValue()).getText());
            } else if (entry.getValue() instanceof SQLNumberExpression) {
                result.put(entry.getKey().getName(), ((SQLNumberExpression) entry.getValue()).getNumber());
            }
        }
        return result;
    }
    
    @Override
    public Optional<String> revertSQL() {
        if (sqlRevertContext.getUndoData().isEmpty()) {
            return Optional.absent();
        }
        sqlBuilder.appendLiterals(DefaultKeyword.UPDATE);
        sqlBuilder.appendLiterals(sqlRevertContext.getActualTable());
        sqlBuilder.appendUpdateSetAssignments(sqlRevertContext.getUpdateSetAssignments().keySet());
        sqlBuilder.appendWhereCondition(sqlRevertContext.getPrimaryKeyColumns());
        return Optional.of(sqlBuilder.toSQL());
    }
    
    @Override
    public void fillParameters(final List<Collection<Object>> revertParameters) {
        for (Map<String, Object> each : sqlRevertContext.getUndoData()) {
            revertParameters.add(getParameters(each));
        }
    }
    
    private List<Object> getParameters(final Map<String, Object> undoRecord) {
        List<Object> result = new LinkedList<>();
        for (String each : sqlRevertContext.getUpdateSetAssignments().keySet()) {
            result.add(undoRecord.get(each.toLowerCase()));
        }
        for (String each : sqlRevertContext.getPrimaryKeyColumns()) {
            Object value = sqlRevertContext.getUpdateSetAssignments().get(each);
            if (null != value) {
                result.add(value);
            } else {
                result.add(undoRecord.get(each));
            }
        }
        return result;
    }
}
