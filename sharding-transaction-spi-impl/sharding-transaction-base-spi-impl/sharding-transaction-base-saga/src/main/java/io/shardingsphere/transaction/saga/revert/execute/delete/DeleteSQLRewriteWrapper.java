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

package io.shardingsphere.transaction.saga.revert.execute.delete;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.execute.SQLRewriteWrapper;
import io.shardingsphere.transaction.saga.revert.snapshot.DMLSnapshotAccessor;
import io.shardingsphere.transaction.saga.revert.snapshot.GenericSQLBuilder;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Delete SQL rewrite wrapper.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class DeleteSQLRewriteWrapper implements SQLRewriteWrapper {
    
    private DeleteSQLRevertContext revertSQLContext;
    
    private final GenericSQLBuilder sqlBuilder = new GenericSQLBuilder();
    
    public DeleteSQLRewriteWrapper(final DMLSnapshotAccessor snapshotDataAccessor) throws SQLException {
        revertSQLContext = new DeleteSQLRevertContext(snapshotDataAccessor.getSnapshotSQLStatement().getTableName(), snapshotDataAccessor.queryUndoData());
    }
    
    @Override
    public Optional<String> revertSQL() {
        if (revertSQLContext.getUndoData().isEmpty()) {
            return Optional.absent();
        }
        sqlBuilder.appendLiterals(DefaultKeyword.INSERT);
        sqlBuilder.appendLiterals(DefaultKeyword.INTO);
        sqlBuilder.appendLiterals(revertSQLContext.getActualTable());
        sqlBuilder.appendInsertValues(revertSQLContext.getUndoData().iterator().next().size());
        return Optional.of(sqlBuilder.toSQL());
    }
    
    @Override
    public void fillParameters(final List<Collection<Object>> revertParameters) {
        for (Map<String, Object> each : revertSQLContext.getUndoData()) {
            revertParameters.add(each.values());
        }
    }
}
