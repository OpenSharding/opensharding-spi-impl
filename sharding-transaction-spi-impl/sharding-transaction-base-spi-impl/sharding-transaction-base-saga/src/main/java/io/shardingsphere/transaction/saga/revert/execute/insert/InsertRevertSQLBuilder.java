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

package io.shardingsphere.transaction.saga.revert.execute.insert;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.shardingsphere.transaction.saga.revert.execute.RevertSQLBuilder;
import io.shardingsphere.transaction.saga.revert.snapshot.SQLBuilder;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Insert revert SQL builder.
 *
 * @author duhongjun
 * @author zhaojun
 */
@RequiredArgsConstructor
public final class InsertRevertSQLBuilder implements RevertSQLBuilder {
    
    private final InsertRevertSQLContext revertSQLContext;
    
    private SQLBuilder sqlBuilder = new SQLBuilder();
    
    @Override
    public Optional<String> generateSQL() {
        Preconditions.checkState(!revertSQLContext.getPrimaryKeyInsertValues().isEmpty(),
            "Could not found primary key values. datasource:[%s], table:[%s]", revertSQLContext.getDataSourceName(), revertSQLContext.getActualTable());
        sqlBuilder.appendLiterals(DefaultKeyword.DELETE);
        sqlBuilder.appendLiterals(DefaultKeyword.FROM);
        sqlBuilder.appendLiterals(revertSQLContext.getActualTable());
        sqlBuilder.appendWhereCondition(revertSQLContext.getPrimaryKeyInsertValues().iterator().next().keySet());
        return Optional.of(sqlBuilder.toSQL());
    }
    
    @Override
    public void fillParameters(final List<Collection<Object>> revertParameters) {
        for (Map<String, Object> each : revertSQLContext.getPrimaryKeyInsertValues()) {
            revertParameters.add(each.values());
        }
    }
}
