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
import io.shardingsphere.transaction.saga.revert.engine.RevertSQLUnit;
import io.shardingsphere.transaction.saga.revert.execute.RevertSQLExecuteWrapper;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.parse.old.lexer.token.DefaultKeyword;

import java.util.Collection;
import java.util.List;

/**
 * Insert revert SQL execute wrapper.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class InsertRevertSQLExecuteWrapper implements RevertSQLExecuteWrapper {
    
    private InsertRevertSQLContext revertSQLContext;
    
    public InsertRevertSQLExecuteWrapper(final String dataSourceName, final String actualTableName, final List<String> primaryKeys, final InsertOptimizeResult insertOptimizeResult) {
        revertSQLContext = new InsertRevertSQLContext(dataSourceName, actualTableName, primaryKeys, insertOptimizeResult);
    }
    
    @Override
    public Optional<RevertSQLUnit> generateRevertSQL() {
        RevertSQLUnit result = new RevertSQLUnit(generateSQL(revertSQLContext));
        for (Collection<Object> each : revertSQLContext.getPrimaryKeyValues().values()) {
            result.getRevertParams().add(each);
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
        for (String each : revertSQLContext.getPrimaryKeyValues().keySet()) {
            if (firstItem) {
                firstItem = false;
                builder.append(" ").append(each).append(" =?");
            } else {
                builder.append(" ").append(DefaultKeyword.AND).append(each).append(" =?");
            }
        }
        return builder.toString();
    }
}
