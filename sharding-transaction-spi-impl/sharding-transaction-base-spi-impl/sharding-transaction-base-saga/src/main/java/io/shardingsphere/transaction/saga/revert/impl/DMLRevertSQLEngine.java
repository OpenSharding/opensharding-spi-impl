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

package io.shardingsphere.transaction.saga.revert.impl;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLEngine;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLExecuteWrapper;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract revert operate.
 *
 * @author duhongjun
 * @author zhaojun
 */
@RequiredArgsConstructor
public class DMLRevertSQLEngine implements RevertSQLEngine {
    
    private final RevertSQLExecuteWrapper revertSQLExecuteWrapper;
    
    private final TableMetaData tableMetaData;
    
    /**
     * Execute revert SQL.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Optional<RevertSQLUnit> execute() throws SQLException {
        List<String> primaryKeyColumns = getPrimaryKeyColumns();
        if (primaryKeyColumns.isEmpty()) {
            throw new RuntimeException("Not supported table without primary key");
        }
        RevertSQLContext revertSQLContext = revertSQLExecuteWrapper.createRevertSQLContext(primaryKeyColumns);
        return revertSQLExecuteWrapper.generateRevertSQL(revertSQLContext);
    }
    
    private List<String> getPrimaryKeyColumns() {
        List<String> result = new ArrayList<>();
        for (ColumnMetaData each : tableMetaData.getColumns().values()) {
            if (each.isPrimaryKey()) {
                result.add(each.getColumnName());
            }
        }
        return result;
    }
}
