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
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaLogicSQLTransaction;
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
    public SagaResponse with(final String datasourceName, final String sql, final List<List<String>> parameterSets) {
        if (Strings.isNullOrEmpty(sql)) {
            return new SuccessfulSagaResponse("Skip empty transaction/compensation");
        }
        if (SagaDefinitionBuilder.ROLLBACK_TAG.equals(sql)) {
            throw new TransportFailedException("Forced Rollback tag has been checked, saga will rollback this transaction");
        }
        Optional<SagaBranchTransaction> branchTransaction = getBranchTransaction(datasourceName, sql, parameterSets);
        return branchTransaction.isPresent() && isNeedExecute(branchTransaction.get()) ? executeSQL(branchTransaction.get()) : new JsonSuccessfulSagaResponse("{}");
    }
    
//    private List<List<Object>> transferList(final List<List<String>> origin) {
//        List<List<Object>> result = Lists.newArrayList();
//        for (List<String> each : origin) {
//            result.add(Lists.<Object>newArrayList(each));
//        }
//        return result;
//    }
    
    private Optional<SagaBranchTransaction> getBranchTransaction(final String datasourceName, final String sql, final List<List<String>> parameterSets) {
        Optional<SagaBranchTransaction> result = Optional.absent();
        for (SagaLogicSQLTransaction each : sagaTransaction.getLogicSQLTransactions()) {
            result = doGetBranchTransaction(each, datasourceName, sql, parameterSets);
            if (result.isPresent()) {
                return result;
            }
        }
        return result;
    }
    
    private Optional<SagaBranchTransaction> doGetBranchTransaction(final SagaLogicSQLTransaction logicSQLTransaction,
                                                                   final String datasourceName, final String sql, final List<List<String>> parameterSets) {
        for (SagaBranchTransaction each : logicSQLTransaction.getBranchTransactions()) {
            if (datasourceName.equals(each.getDataSourceName()) && sql.equals(each.getSql()) && parameterSets.equals(each.getParameterSets())) {
                return Optional.of(each);
            }
        }
        return Optional.absent();
    }
   
    private boolean isNeedExecute(final SagaBranchTransaction branchTransaction) {
        ExecuteStatus executeStatus = branchTransaction.getExecuteStatus();
        if (null == executeStatus || ExecuteStatus.COMPENSATING == executeStatus) {
            return true;
        }
        if (ExecuteStatus.SUCCESS == executeStatus) {
            return false;
        }
        branchTransaction.setExecuteStatus(ExecuteStatus.COMPENSATING);
        throw new TransportFailedException(String.format("branchTransaction %s execute failed, need to compensate", branchTransaction.toString()));
    }
        
    private SagaResponse executeSQL(final SagaBranchTransaction branchTransaction) {
        Connection connection = getConnection(branchTransaction.getDataSourceName());
        try (PreparedStatement preparedStatement = connection.prepareStatement(branchTransaction.getSql())) {
            if (branchTransaction.getParameterSets().isEmpty()) {
                preparedStatement.executeUpdate();
            } else {
                executeBatch(preparedStatement, branchTransaction.getParameterSets());
            }
        } catch (SQLException ex) {
            throw new TransportFailedException(String.format("Execute SQL `%s` occur exception.", branchTransaction.toString()), ex);
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
