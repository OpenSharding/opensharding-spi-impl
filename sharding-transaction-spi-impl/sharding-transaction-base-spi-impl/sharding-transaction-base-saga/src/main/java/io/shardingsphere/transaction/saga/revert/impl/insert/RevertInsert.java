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

import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.impl.AbstractRevertOperate;
import io.shardingsphere.transaction.saga.revert.impl.RevertContextGeneratorParameter;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.insertvalue.InsertValue;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Revert insert.
 *
 * @author duhongjun
 */
public final class RevertInsert extends AbstractRevertOperate {
    
    public RevertInsert() {
        this.setRevertSQLGenerator(new RevertInsertGenerator());
    }
    
    @Override
    protected RevertContextGeneratorParameter createRevertContext(final SnapshotParameter snapshotParameter, final List<String> keys) {
        InsertStatement insertStatement = (InsertStatement) snapshotParameter.getStatement();
        RevertInsertGeneratorParameter result = new RevertInsertGeneratorParameter(snapshotParameter.getActualTable(), insertStatement.getColumnNames(), keys, snapshotParameter.getActualSQLParams(),
                insertStatement.getValues().size(), insertStatement.getGeneratedKeyConditions().isEmpty());
        Iterator<String> columnNamesIterator = insertStatement.getColumnNames().iterator();
        Iterator actualSQLParameterIterator = snapshotParameter.getActualSQLParams().iterator();
        for (InsertValue each : insertStatement.getValues()) {
            result.getInsertGroups().add(createInsertGroup(each, columnNamesIterator, actualSQLParameterIterator, keys));
        }
        return result;
    }
    
    private Map<String, Object> createInsertGroup(final InsertValue insertValue, final Iterator<String> columnNamesIterator, final Iterator actualSQLParameterIterator, final List<String> keys) {
        Map<String, Object> result = new HashMap<>();
        for (SQLExpression expression : insertValue.getColumnValues()) {
            String columnName = columnNamesIterator.next();
            if (!keys.contains(columnName)) {
                continue;
            }
            if (expression instanceof SQLPlaceholderExpression) {
                result.put(columnName, actualSQLParameterIterator.next());
            } else if (expression instanceof SQLTextExpression) {
                result.put(columnName, ((SQLTextExpression) expression).getText());
            } else if (expression instanceof SQLNumberExpression) {
                result.put(columnName, ((SQLNumberExpression) expression).getNumber());
            }
        }
        return result;
    }
}
