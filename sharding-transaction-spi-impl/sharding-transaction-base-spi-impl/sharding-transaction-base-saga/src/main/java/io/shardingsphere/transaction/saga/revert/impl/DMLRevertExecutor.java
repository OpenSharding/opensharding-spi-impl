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
import io.shardingsphere.transaction.saga.revert.api.RevertExecutor;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract revert operate.
 *
 * @author duhongjun
 */
@Setter
@Getter
@RequiredArgsConstructor
public abstract class DMLRevertExecutor implements RevertExecutor {
    
    private final RevertSQLGenerator revertSQLGenerator;
    
    /**
     * Execute.
     *
     * @param tableMetaData table meta data
     */
    @Override
    public Optional<RevertSQLUnit> execute(final TableMetaData tableMetaData) throws SQLException {
        List<String> keys = getKeyColumns(tableMetaData);
        if (keys.isEmpty()) {
            throw new RuntimeException("Not supported table without primary key");
        }
        return revertSQLGenerator.generateRevertSQL(buildRevertSQLStatement(keys));
    }
    
    private List<String> getKeyColumns(final TableMetaData tableMetaData) {
        List<String> result = new ArrayList<>();
        for (ColumnMetaData each : tableMetaData.getColumns().values()) {
            if (each.isPrimaryKey()) {
                result.add(each.getColumnName());
            }
        }
        return result;
    }
    
    protected abstract RevertSQLStatement buildRevertSQLStatement(List<String> keys) throws SQLException;
}
