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

package io.shardingsphere.transaction.saga.revert.impl.delete;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.DMLSnapshotAccessor;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLExecuteWrapper;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.sql.SQLException;
import java.util.Map;

/**
 * Delete revert SQL execute wrapper.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class DeleteRevertSQLExecuteWrapper implements RevertSQLExecuteWrapper<DeleteRevertSQLContext> {
    
    private String actualTableName;
    
    private final DMLSnapshotAccessor snapshotDataAccessor;
    
    public DeleteRevertSQLExecuteWrapper(final DMLSnapshotAccessor snapshotDataAccessor) {
        this.snapshotDataAccessor = snapshotDataAccessor;
        actualTableName = snapshotDataAccessor.getSnapshotSQLStatement().getActualTableName();
    }
    
    @Override
    public DeleteRevertSQLContext createRevertSQLContext() throws SQLException {
        return new DeleteRevertSQLContext(actualTableName, snapshotDataAccessor.queryUndoData());
    }
    
    @Override
    public Optional<RevertSQLUnit> generateRevertSQL(final DeleteRevertSQLContext revertSQLContext) {
        if (revertSQLContext.getUndoData().isEmpty()) {
            return Optional.absent();
        }
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultKeyword.INSERT).append(" ");
        builder.append(DefaultKeyword.INTO).append(" ");
        builder.append(revertSQLContext.getActualTable()).append(" ");
        builder.append(DefaultKeyword.VALUES).append(" ");
        builder.append("(");
        int columnCount = revertSQLContext.getUndoData().get(0).size();
        for (int i = 0; i < columnCount; i++) {
            builder.append("?");
            if (i < columnCount - 1) {
                builder.append(",");
            }
        }
        builder.append(")");
        RevertSQLUnit result = new RevertSQLUnit(builder.toString());
        for (Map<String, Object> each : revertSQLContext.getUndoData()) {
            result.getRevertParams().add(each.values());
        }
        return Optional.of(result);
    }
}
