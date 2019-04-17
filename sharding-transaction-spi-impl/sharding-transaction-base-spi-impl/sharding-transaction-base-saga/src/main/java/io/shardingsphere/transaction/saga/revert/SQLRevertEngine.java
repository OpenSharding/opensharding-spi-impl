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

package io.shardingsphere.transaction.saga.revert;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaBranchTransactionGroup;
import io.shardingsphere.transaction.saga.revert.api.RevertExecutor;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import io.shardingsphere.transaction.saga.revert.impl.RevertOperateFactory;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DMLStatement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * SQL revert engine.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
public final class SQLRevertEngine {
    
    private final Map<String, Connection> connectionMap;
    
    private final RevertOperateFactory revertOperateFactory = new RevertOperateFactory();
    
    /**
     * Get revert result.
     *
     * @param sagaBranchTransaction saga branch transaction
     * @param sagaBranchTransactionGroup saga branch transaction group
     * @return revert result
     * @throws SQLException SQL exception
     */
    public SQLRevertResult revert(final SagaBranchTransaction sagaBranchTransaction, final SagaBranchTransactionGroup sagaBranchTransactionGroup) throws SQLException {
        SQLRevertResult result = new SQLRevertResult();
        DMLStatement dmlStatement = (DMLStatement) sagaBranchTransactionGroup.getSqlStatement();
        TableMetaData tableMetaData = sagaBranchTransactionGroup.getShardingTableMetaData().get(dmlStatement.getTables().getSingleTableName());
        Connection actualConnection = connectionMap.get(sagaBranchTransaction.getDataSourceName());
        String actualTableName = sagaBranchTransaction.getActualTableName();
        for (List<Object> each : sagaBranchTransaction.getParameterSets()) {
            RevertExecutor revertOperate = revertOperateFactory.getRevertSQLCreator(actualTableName, dmlStatement, each, tableMetaData, actualConnection);
            Optional<RevertSQLUnit> revertContextOptional = revertOperate.execute(tableMetaData);
            if (revertContextOptional.isPresent()) {
                result.setSql(revertContextOptional.get().getRevertSQL());
                result.getParameterSets().addAll(revertContextOptional.get().getRevertParams());
            }
        }
        return result;
    }
}
