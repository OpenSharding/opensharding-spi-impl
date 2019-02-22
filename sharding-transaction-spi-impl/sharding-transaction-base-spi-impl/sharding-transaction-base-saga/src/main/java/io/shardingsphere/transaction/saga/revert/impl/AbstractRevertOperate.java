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
import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.api.RevertOperate;
import io.shardingsphere.transaction.saga.revert.api.RevertParameter;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract revert operate.
 *
 * @author duhongjun
 */
@Getter
@Setter
public abstract class AbstractRevertOperate implements RevertOperate {
    
    private RevertContextGenerator revertSQLGenerator;
    
    // CHECKSTYLE:OFF
    @Override
    // CHECKSTYLE:ON
    public Optional<RevertContext> snapshot(final SnapshotParameter snapshotParameter) throws SQLException {
        List<String> keys = getKeyColumns(snapshotParameter);
        if (keys.isEmpty()) {
            throw new RuntimeException("Not supported table witout primary key");
        }
        return revertSQLGenerator.generate(createRevertContext(snapshotParameter, keys));
    }
    
    private List<String> getKeyColumns(final SnapshotParameter snapshotParameter) {
        List<String> result = new ArrayList<>();
        for (ColumnMetaData each : snapshotParameter.getTableMeta().getColumns().values()) {
            if (each.isPrimaryKey()) {
                result.add(each.getColumnName());
            }
        }
        return result;
    }
    
    protected abstract RevertContextGeneratorParameter createRevertContext(SnapshotParameter snapshotParameter, List<String> keys) throws SQLException;
    
    // CHECKSTYLE:OFF
    @Override
    // CHECKSTYLE:ON
    public void revert(final RevertParameter parameter) throws SQLException {
        JDBCUtil.executeUpdate(parameter.getConnection(), parameter.getSql(), parameter.getParams());
    }
}
