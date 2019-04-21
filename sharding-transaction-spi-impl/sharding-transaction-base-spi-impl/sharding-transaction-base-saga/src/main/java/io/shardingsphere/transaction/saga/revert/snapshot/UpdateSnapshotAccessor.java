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

package io.shardingsphere.transaction.saga.revert.snapshot;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.executor.SQLRevertExecutorContext;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Update snapshot accessor.
 *
 * @author zhaojun
 */
public final class UpdateSnapshotAccessor extends DMLSnapshotAccessor {
    
    private UpdateStatement updateStatement;
    
    private List<String> primaryKeyColumns;
    
    public UpdateSnapshotAccessor(final SQLRevertExecutorContext executorContext) {
        super(executorContext);
        this.updateStatement = (UpdateStatement) executorContext.getSqlStatement();
        this.primaryKeyColumns = executorContext.getPrimaryKeyColumns();
    }
    
    @Override
    protected SnapshotSQLContext getSnapshotSQLContext(final SQLRevertExecutorContext context) {
        return new SnapshotSQLContext(context.getConnection(), context.getActualTableName(), context.getParameters(), getQueryColumnNames(), getTableAlias(), getWhereClause());
    }
    
    private Collection<String> getQueryColumnNames() {
        Collection<String> result = new LinkedList<>();
        if (primaryKeyColumns.isEmpty()) {
            return Collections.singleton("*");
        }
        List<String> remainPrimaryKeys = new LinkedList<>(primaryKeyColumns);
        for (Column each : updateStatement.getAssignments().keySet()) {
            int dotPosition = each.getName().indexOf('.');
            String columnName = dotPosition > 0 ? each.getName().substring(dotPosition + 1).toLowerCase() : each.getName().toLowerCase();
            result.add(columnName);
            remainPrimaryKeys.remove(columnName);
        }
        result.addAll(remainPrimaryKeys);
        return result;
    }
    
    private String getTableAlias() {
        String result = null;
        Optional<Table> table = updateStatement.getTables().find(updateStatement.getTables().getSingleTableName());
        if (table.isPresent() && table.get().getAlias().isPresent() && !table.get().getAlias().get().equals(table.get().getName())) {
            result = table.get().getAlias().get();
        }
        return result;
    }
    
    private String getWhereClause() {
        return 0 < updateStatement.getWhereStartIndex() ? updateStatement.getLogicSQL().substring(updateStatement.getWhereStartIndex(), updateStatement.getWhereStopIndex() + 1) : "";
    }
}
