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

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.impl.delete.DeleteSnapshotMaker;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.condition.Column;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Table;

import java.util.LinkedList;
import java.util.List;

/**
 * Update snapshot maker.
 *
 * @author duhongjun
 */
public final class UpdateSnapshotMaker extends DeleteSnapshotMaker {
    
    @Override
    protected void fillAlias(final StringBuilder builder, final SnapshotParameter snapshotParameter) {
        DMLStatement dmlStatement = snapshotParameter.getStatement();
        Optional<Table> table = dmlStatement.getTables().find(dmlStatement.getTables().getSingleTableName());
        if (table.isPresent() && table.get().getAlias().isPresent() && !table.get().getAlias().get().equals(table.get().getName())) {
            builder.append(" ").append(table.get().getAlias().get()).append(" ");
        }
    }
    
    @Override
    protected void fillSelectItem(final StringBuilder builder, final SnapshotParameter snapshotParameter, final List<String> keys) {
        if (keys.isEmpty()) {
            super.fillSelectItem(builder, snapshotParameter, keys);
            return;
        }
        List<String> tableKeys = new LinkedList<>(keys);
        boolean first = true;
        UpdateStatement updateStatement = (UpdateStatement) snapshotParameter.getStatement();
        for (Column each : updateStatement.getAssignments().keySet()) {
            int dotPos = each.getName().indexOf('.');
            String realColumnName;
            if (dotPos > 0) {
                realColumnName = each.getName().substring(dotPos + 1).toLowerCase();
            } else {
                realColumnName = each.getName().toLowerCase();
            }
            if (first) {
                first = false;
            } else {
                builder.append(", ");
            }
            builder.append(realColumnName);
            tableKeys.remove(realColumnName);
        }
        for (String each : tableKeys) {
            builder.append(", ");
            builder.append(each);
        }
        builder.append(" ");
    }
}
