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

package io.shardingsphere.transaction.saga.revert.impl.insert;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.shardingsphere.core.parsing.lexer.token.DefaultKeyword;

import com.google.common.base.Optional;

import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.impl.RevertContextGenerator;
import io.shardingsphere.transaction.saga.revert.impl.RevertContextGeneratorParameter;

/**
 * Revert insert generator.
 *
 * @author duhongjun
 */
public final class RevertInsertGenerator implements RevertContextGenerator {
    
    @Override
    public Optional<RevertContext> generate(final RevertContextGeneratorParameter parameter) throws SQLException {
        RevertInsertGeneratorParameter insertParameter = (RevertInsertGeneratorParameter) parameter;
        RevertContext result = new RevertContext(generateSQL(insertParameter));
        Set<Integer> keyColumnIndexs = new HashSet<>();
        List<String> allColumns = new LinkedList<>();
        int index = 0;
        for (String each : insertParameter.getTableColumns()) {
            allColumns.add(each);
            for (String key : insertParameter.getKeys()) {
                if (key.equalsIgnoreCase(each)) {
                    keyColumnIndexs.add(index);
                    break;
                }
            }
            index++;
        }
        fillRevertParams(result, insertParameter, keyColumnIndexs);
        return Optional.of(result);
    }
    
    private String generateSQL(final RevertContextGeneratorParameter parameter) {
        RevertInsertGeneratorParameter insertParameter = (RevertInsertGeneratorParameter) parameter;
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultKeyword.DELETE).append(" ");
        builder.append(DefaultKeyword.FROM).append(" ");
        builder.append(insertParameter.getActualTable()).append(" ");
        builder.append(DefaultKeyword.WHERE).append(" ");
        int pos = 0;
        for (Object key : insertParameter.getKeys()) {
            if (pos > 0) {
                builder.append(" ").append(DefaultKeyword.AND).append(" ");
            }
            builder.append(key).append(" = ?");
            pos++;
        }
        return builder.toString();
    }
    
    private void fillRevertParams(final RevertContext revertContext, final RevertInsertGeneratorParameter insertParameter, final Set<Integer> keyColumnIndexs) {
        if (insertParameter.isGenerateKey()) {
            for (int i = 0; i < insertParameter.getBatchSize(); i++) {
                Collection<Object> currentSQLParams = new LinkedList<>();
                int generateValueIndex = (i + 1) * insertParameter.getBatchSize() - 1;
                currentSQLParams.add(insertParameter.getParams().get(generateValueIndex));
                revertContext.getRevertParams().add(currentSQLParams);
            }
            return;
        }
        for (Map<String, Object> each : insertParameter.getKeyValues()) {
            Collection<Object> currentSQLParams = new LinkedList<>();
            revertContext.getRevertParams().add(currentSQLParams);
            for (Entry<String, Object> eachEntry : each.entrySet()) {
                currentSQLParams.add(eachEntry.getValue());
            }
        }
    }
}
