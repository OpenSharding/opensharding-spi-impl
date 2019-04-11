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

package io.shardingsphere.transaction.saga.revert.impl.update;

import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.impl.RevertContextGeneratorParameter;
import io.shardingsphere.transaction.saga.revert.impl.delete.RevertDelete;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLPlaceholderExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Revert update.
 *
 * @author duhongjun
 */
public final class RevertUpdate extends RevertDelete {
    
    public RevertUpdate() {
        this.setSnapshotMaker(new UpdateSnapshotMaker());
        this.setRevertSQLGenerator(new RevertUpdateGenerator());
    }
    
    @Override
    protected RevertContextGeneratorParameter createRevertContext(final SnapshotParameter snapshotParameter, final List<String> keys) throws SQLException {
        List<Map<String, Object>> selectSnapshot = this.getSnapshotMaker().make(snapshotParameter, keys);
        Map<String, Object> updateColumns = new LinkedHashMap<>();
        for (Entry<Column, SQLExpression> entry : snapshotParameter.getStatement().getUpdateColumnValues().entrySet()) {
            if (entry.getValue() instanceof SQLPlaceholderExpression) {
                updateColumns.put(entry.getKey().getName(), snapshotParameter.getActualSQLParams().get(((SQLPlaceholderExpression) entry.getValue()).getIndex()));
            } else if (entry.getValue() instanceof SQLTextExpression) {
                updateColumns.put(entry.getKey().getName(), ((SQLTextExpression) entry.getValue()).getText());
            } else if (entry.getValue() instanceof SQLNumberExpression) {
                updateColumns.put(entry.getKey().getName(), ((SQLNumberExpression) entry.getValue()).getNumber());
            }
        }
        return new RevertUpdateGeneratorParameter(snapshotParameter.getActualTable(), selectSnapshot, updateColumns, keys, snapshotParameter.getActualSQLParams());
    }
}
