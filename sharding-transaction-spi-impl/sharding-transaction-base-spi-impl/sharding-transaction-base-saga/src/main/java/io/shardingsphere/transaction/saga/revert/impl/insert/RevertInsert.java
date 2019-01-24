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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.shardingsphere.core.parsing.parser.context.condition.Column;
import org.apache.shardingsphere.core.parsing.parser.context.insertvalue.InsertValue;
import org.apache.shardingsphere.core.parsing.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parsing.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parsing.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parsing.parser.expression.SQLTextExpression;
import org.apache.shardingsphere.core.parsing.parser.sql.dml.insert.InsertStatement;

import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.impl.AbstractRevertOperate;
import io.shardingsphere.transaction.saga.revert.impl.RevertContextGeneratorParameter;

/**
 * Revert insert.
 *
 * @author duhongjun
 */
public final class RevertInsert extends AbstractRevertOperate {
    
    public RevertInsert() {
        this.setRevertSQLGenerator(new RevertInsertGenerator());
    }
    
    protected RevertContextGeneratorParameter createRevertContext(final SnapshotParameter snapshotParameter, final List<String> keys) throws SQLException {
        List<String> tableColumns = new LinkedList<>();
        InsertStatement insertStatement = (InsertStatement) snapshotParameter.getStatement();
        for (Column each : insertStatement.getColumns()) {
            tableColumns.add(each.getName());
        }
        RevertInsertGeneratorParameter result = new RevertInsertGeneratorParameter(snapshotParameter.getActualTable(), tableColumns, keys, snapshotParameter.getActualSQLParams(),
                insertStatement.getInsertValues().getInsertValues().size(), -1 != insertStatement.getGenerateKeyColumnIndex());
        for (InsertValue each : insertStatement.getInsertValues().getInsertValues()) {
            Map<String, Object> keyValue = new HashMap<>();
            result.getKeyValues().add(keyValue);
            int index = 0;
            for (SQLExpression expression : each.getColumnValues()) {
                Column column = insertStatement.getColumns().get(index++);
                if (!keys.contains(column.getName())) {
                    continue;
                }
                if (expression instanceof SQLPlaceholderExpression) {
                    keyValue.put(column.getName(), snapshotParameter.getActualSQLParams().get(((SQLPlaceholderExpression) expression).getIndex()));
                } else if (expression instanceof SQLTextExpression) {
                    keyValue.put(column.getName(), ((SQLTextExpression) expression).getText());
                } else if (expression instanceof SQLNumberExpression) {
                    keyValue.put(column.getName(), ((SQLNumberExpression) expression).getNumber());
                }
            }
        }
        return result;
    }
}
