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
import java.util.LinkedList;
import java.util.List;

import org.apache.shardingsphere.core.parsing.parser.context.condition.Column;
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
        for (Column each : ((InsertStatement) snapshotParameter.getStatement()).getColumns()) {
            tableColumns.add(each.getName());
        }
        return new RevertInsertGeneratorParameter(snapshotParameter.getActualTable(), tableColumns, keys, snapshotParameter.getActualSQLParams());
    }
}
