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
import io.shardingsphere.transaction.saga.revert.engine.RevertSQLUnit;
import io.shardingsphere.transaction.saga.revert.execute.RevertSQLExecuteWrapper;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.util.Map;

/**
 * Insert revert SQL execute wrapper.
 *
 * @author duhongjun
 * @author zhaojun
 */
@RequiredArgsConstructor
public final class InsertRevertSQLExecuteWrapper implements RevertSQLExecuteWrapper {
    
    private final InsertRevertSQLContext revertSQLContext;
    
    @Override
    public Optional<RevertSQLUnit> generateRevertSQL() {
        Preconditions.checkState(!revertSQLContext.getPrimaryKeyInsertValues().isEmpty(),
            "Could not found primary key values. datasource:[%s], table:[%s]", revertSQLContext.getDataSourceName(), revertSQLContext.getActualTable());
        RevertSQLUnit result = new RevertSQLUnit(generateSQL(revertSQLContext));
        for (Map<String, Object> each : revertSQLContext.getPrimaryKeyInsertValues()) {
            result.getRevertParams().add(each.values());
        }
        return Optional.of(result);
    }
    
    private String generateSQL(final InsertRevertSQLContext revertSQLContext) {
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultKeyword.DELETE).append(" ");
        builder.append(DefaultKeyword.FROM).append(" ");
        builder.append(revertSQLContext.getActualTable()).append(" ");
        builder.append(DefaultKeyword.WHERE).append(" ");
        boolean firstItem = true;
        for (String each : revertSQLContext.getPrimaryKeyInsertValues().iterator().next().keySet()) {
            if (firstItem) {
                firstItem = false;
                builder.append(each).append(" =?");
            } else {
                builder.append(" ").append(DefaultKeyword.AND).append(" ").append(each).append(" =?");
            }
        }
        return builder.toString();
    }
}
