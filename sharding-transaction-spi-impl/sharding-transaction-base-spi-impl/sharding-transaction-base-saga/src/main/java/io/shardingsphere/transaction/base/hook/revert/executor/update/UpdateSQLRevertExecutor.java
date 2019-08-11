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

package io.shardingsphere.transaction.base.hook.revert.executor.update;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.base.hook.revert.GenericSQLBuilder;
import io.shardingsphere.transaction.base.hook.revert.RevertSQLResult;
import io.shardingsphere.transaction.base.hook.revert.constant.DefaultKeyword;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutor;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import io.shardingsphere.transaction.base.hook.revert.snapshot.UpdateSnapshotAccessor;
import org.apache.shardingsphere.core.parse.sql.segment.dml.assignment.AssignmentSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.complex.CommonExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.UpdateStatement;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Update SQL revert executor.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class UpdateSQLRevertExecutor implements SQLRevertExecutor {
    
    private UpdateSQLRevertContext sqlRevertContext;
    
    private final GenericSQLBuilder sqlBuilder = new GenericSQLBuilder();
    
    public UpdateSQLRevertExecutor(final SQLRevertExecutorContext context, final UpdateSnapshotAccessor snapshotAccessor) throws SQLException {
        sqlRevertContext = createRevertSQLContext(context, snapshotAccessor);
    }
    
    private UpdateSQLRevertContext createRevertSQLContext(final SQLRevertExecutorContext context, final UpdateSnapshotAccessor snapshotAccessor) throws SQLException {
        Map<String, Object> updateSetAssignments = getUpdateSetAssignments((UpdateStatement) context.getOptimizedStatement().getSQLStatement(), context.getParameters());
        return new UpdateSQLRevertContext(context.getActualTableName(), snapshotAccessor.queryUndoData(), updateSetAssignments, context.getPrimaryKeyColumns(), context.getParameters());
    }
    
    private Map<String, Object> getUpdateSetAssignments(final UpdateStatement updateStatement, final List<Object> parameters) {
        Map<String, Object> result = new LinkedHashMap<>();
        for(AssignmentSegment each : updateStatement.getSetAssignment().getAssignments()){
            if (each.getValue() instanceof ParameterMarkerExpressionSegment) {
                result.put(each.getColumn().getName(), parameters.get(((ParameterMarkerExpressionSegment) each.getValue()).getParameterMarkerIndex()));
            } else if (each.getValue() instanceof CommonExpressionSegment) {
                result.put(each.getColumn().getName(), ((CommonExpressionSegment) each.getValue()).getText());
            } else if (each.getValue() instanceof LiteralExpressionSegment) {
                result.put(each.getColumn().getName(), ((LiteralExpressionSegment) each.getValue()).getLiterals());
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
    public void fillParameters(final RevertSQLResult revertSQLResult) {
        for (Map<String, Object> each : sqlRevertContext.getUndoData()) {
            revertSQLResult.getParameters().add(getParameters(each));
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
