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

package io.shardingsphere.transaction.saga.servicecomb.transport;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
import lombok.RequiredArgsConstructor;
import org.apache.servicecomb.saga.core.SagaResponse;
import org.apache.servicecomb.saga.core.SuccessfulSagaResponse;
import org.apache.servicecomb.saga.core.TransportFailedException;
import org.apache.servicecomb.saga.format.JsonSuccessfulSagaResponse;
import org.apache.servicecomb.saga.transports.SQLTransport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Sharding SQL transport.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
public final class ShardingSQLTransport implements SQLTransport {
    
    private final SagaTransaction sagaTransaction;
    
    @Override
    public SagaResponse with(final String datasourceName, final String sql, final List<List<String>> sagaParameters) {
        if (Strings.isNullOrEmpty(sql)) {
            return new SuccessfulSagaResponse("Skip empty transaction/compensation");
        }
        if (SagaDefinitionBuilder.ROLLBACK_TAG.equals(sql)) {
            sagaTransaction.changeAllLogicTransactionStatus(ExecuteStatus.COMPENSATING);
            throw new TransportFailedException("Forced Rollback tag has been checked, saga will rollback this transaction");
        }
        Optional<SagaBranchTransaction> branchTransaction = sagaTransaction.findBranchTransaction(datasourceName, sql, sagaParameters);
        return branchTransaction.isPresent() && branchTransaction.get().isExecuteSQL() ? executeSQL(datasourceName, sql, sagaParameters) : new JsonSuccessfulSagaResponse("{}");
    }
    
//    private boolean isExecuteSQL(final ExecuteStatus executeStatus) {
//        return ExecuteStatus.COMPENSATING.equals(executeStatus) || ExecuteStatus.FAILURE.equals(executeStatus);
//        ExecuteStatus executeStatus = branchTransaction.getExecuteStatus();
//        if (null == executeStatus || ExecuteStatus.COMPENSATING == executeStatus) {
//            return true;
//        }
//        if (ExecuteStatus.SUCCESS == executeStatus) {
//            return false;
//        }
//        branchTransaction.setExecuteStatus(ExecuteStatus.COMPENSATING);
//        throw new TransportFailedException(String.format("branchTransaction %s execute failed, need to compensate", branchTransaction.toString()));
//    }
        
    private SagaResponse executeSQL(final String datasourceName, final String sql, final List<List<String>> sagaParameters) {
        Connection connection = getConnection(datasourceName);
        List<List<Object>> sqlParameters = convertSagaParameters(sagaParameters);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (sqlParameters.isEmpty()) {
                preparedStatement.executeUpdate();
            } else {
                executeBatch(preparedStatement, sqlParameters);
            }
        } catch (SQLException ex) {
            throw new TransportFailedException(String.format("Execute SQL `%s` occur exception. dataSourceName:[%s], parameters:[%s]", sql, datasourceName, sagaParameters), ex);
        }
        return new JsonSuccessfulSagaResponse("{}");
    }
    
    private Connection getConnection(final String datasourceName) {
        try {
            Connection result = SagaResourceManager.getTransactionResource(sagaTransaction).getConnectionMap().get(datasourceName);
            if (!result.getAutoCommit()) {
                result.setAutoCommit(true);
            }
            return result;
        } catch (final SQLException ex) {
            throw new TransportFailedException(String.format("Get connection of data source name `%s` occur exception: ", datasourceName), ex);
        }
    }
    
    private List<List<Object>> convertSagaParameters(final List<List<String>> sagaParameters) {
        List<List<Object>> result = Lists.newArrayList();
        for (List<String> each : sagaParameters) {
            result.add(Lists.<Object>newArrayList(each));
        }
        return result;
    }
    
    private void executeBatch(final PreparedStatement preparedStatement, final List<List<Object>> parameterSets) throws SQLException {
        for (List<Object> each : parameterSets) {
            for (int parameterIndex = 0; parameterIndex < each.size(); parameterIndex++) {
                preparedStatement.setObject(parameterIndex + 1, each.get(parameterIndex));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
    }
}
