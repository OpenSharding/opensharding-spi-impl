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

package io.shardingsphere.transaction.base.hook.revert.snapshot;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import org.apache.shardingsphere.core.parse.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DeleteStatement;

import java.util.Collection;
import java.util.Collections;

/**
 * Delete snapshot accessor.
 *
 * @author zhaojun
 */
public final class DeleteSnapshotAccessor extends DMLSnapshotAccessor {
    
    private final DeleteStatement deleteStatement;
    
    public DeleteSnapshotAccessor(final SQLRevertExecutorContext context) {
        super(context);
        this.deleteStatement = (DeleteStatement) context.getShardingStatement().getSQLStatement();
    }
    
    @Override
    public SnapshotSQLContext getSnapshotSQLContext(final SQLRevertExecutorContext context) {
        return new SnapshotSQLContext(context.getConnection(), context.getActualTableName(), context.getParameters(), getQueryColumnNames(), "", getWhereClause());
    }
    
    private Collection<String> getQueryColumnNames() {
        return Collections.singleton("*");
    }
    
    private String getWhereClause() {
        Optional<WhereSegment> whereSegment = deleteStatement.getWhere();
        return whereSegment.isPresent() ? getExecutorContext().getLogicSQL().substring(whereSegment.get().getStartIndex(), whereSegment.get().getStopIndex() + 1) : "";
    }
}
