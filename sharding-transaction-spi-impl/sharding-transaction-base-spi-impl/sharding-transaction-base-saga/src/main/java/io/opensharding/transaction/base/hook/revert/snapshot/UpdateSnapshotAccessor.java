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

package io.opensharding.transaction.base.hook.revert.snapshot;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.opensharding.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import org.apache.shardingsphere.core.parse.sql.segment.dml.assignment.AssignmentSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.core.parse.sql.segment.generic.TableSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.UpdateStatement;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Update snapshot accessor.
 *
 * @author zhaojun
 */
public final class UpdateSnapshotAccessor extends DMLSnapshotAccessor {
    
    private UpdateStatement updateStatement;
    
    public UpdateSnapshotAccessor(final SQLRevertExecutorContext executorContext) {
        super(executorContext);
        this.updateStatement = (UpdateStatement) executorContext.getShardingStatement().getSQLStatement();
    }
    
    @Override
    protected SnapshotSQLContext getSnapshotSQLContext(final SQLRevertExecutorContext context) {
        return new SnapshotSQLContext(context.getConnection(), context.getActualTableName(), getWhereParameters(context),
            getQueryColumnNames(context), getTableAlias().or(""), getWhereClause());
    }
    
    private Collection<String> getQueryColumnNames(final SQLRevertExecutorContext context) {
        Collection<String> result = new LinkedList<>();
        Preconditions.checkState(!context.getPrimaryKeyColumns().isEmpty(),
            "Could not found primary key columns, datasourceName:[%s], tableName:[%s]", context.getDataSourceName(), context.getActualTableName());
        List<String> remainPrimaryKeys = new LinkedList<>(context.getPrimaryKeyColumns());
        for (AssignmentSegment each : updateStatement.getSetAssignment().getAssignments()) {
            result.add(each.getColumn().getName());
            remainPrimaryKeys.remove(each.getColumn().getName());
        }
        result.addAll(remainPrimaryKeys);
        return result;
    }
    
    private Optional<String> getTableAlias() {
        Optional<TableSegment> table = findLogicTable(getExecutorContext().getLogicTableName());
        if (table.isPresent() && table.get().getAlias().isPresent() && !table.get().getAlias().get().equals(table.get().getTableName())) {
            return table.get().getAlias();
        }
        return Optional.absent();
    }
    
    private Optional<TableSegment> findLogicTable(final String logicTableName) {
        for (TableSegment each : updateStatement.getTables()) {
            if (logicTableName.equals(each.getTableName())) {
                return Optional.of(each);
            }
        }
        return Optional.absent();
    }
    
    private String getWhereClause() {
        Optional<WhereSegment> whereSegment = updateStatement.getWhere();
        return whereSegment.isPresent() ? getExecutorContext().getLogicSQL().substring(whereSegment.get().getStartIndex(), whereSegment.get().getStopIndex() + 1) : "";
    }
    
    private Collection<Object> getWhereParameters(final SQLRevertExecutorContext context) {
        Collection<Object> result = new LinkedList<>();
        Optional<WhereSegment> whereSegment = updateStatement.getWhere();
        if (whereSegment.isPresent()) {
            for (int i = whereSegment.get().getParameterStartIndex(); i <= whereSegment.get().getParametersCount(); i++) {
                result.add(context.getParameters().get(i));
            }
        }
        return result;
    }
}
