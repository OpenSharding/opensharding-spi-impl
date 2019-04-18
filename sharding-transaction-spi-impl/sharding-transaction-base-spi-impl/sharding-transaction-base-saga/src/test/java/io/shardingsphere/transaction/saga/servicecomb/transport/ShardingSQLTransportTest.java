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

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.resource.SagaTransactionResource;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.TransportFailedException;
import org.apache.servicecomb.saga.format.JsonSuccessfulSagaResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class ShardingSQLTransportTest {
    
    @Mock
    private PreparedStatement statement;
    
    @Mock
    private SagaTransaction sagaTransaction;
    
    @Mock
    private SagaTransactionResource transactionResource;
    
    private String dataSourceName = "ds";
    
    private String sql = "SELECT * FROM ds.table WHERE id = ? AND column = ?";
    
    @Before
    public void setUp() throws SQLException {
        mockGetConnections();
        mockGetResource();
    }
    
    private void mockGetConnections() throws SQLException {
        ConcurrentMap<String, Connection> connectionMap = new ConcurrentHashMap<>();
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        connectionMap.put(dataSourceName, connection);
        when(transactionResource.getConnectionMap()).thenReturn(connectionMap);
    }
    
    @SneakyThrows
    private void mockGetResource() {
        Field resourceMapField = SagaResourceManager.class.getDeclaredField("TRANSACTION_RESOURCE_MAP");
        resourceMapField.setAccessible(true);
        Map<SagaTransaction, SagaTransactionResource> resourceMap = (Map<SagaTransaction, SagaTransactionResource>) resourceMapField.get(SagaResourceManager.class);
        resourceMap.put(sagaTransaction, transactionResource);
    }
    
    @Test
    public void assertWithSuccessResult() {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = getParameterSets();
        mockExecutionResults(parameterSets, ExecuteStatus.SUCCESS);
        shardingSQLTransport.with(dataSourceName, sql, parameterSets);
        verify(transactionResource, never()).getConnectionMap();
    }
    
    @Test(expected = TransportFailedException.class)
    public void assertWithFailureResult() throws SQLException {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = getParameterSets();
        mockExecutionResults(parameterSets, ExecuteStatus.FAILURE);
        shardingSQLTransport.with(dataSourceName, sql, parameterSets);
    }
    
    @Test
    public void assertWithCompensatingResult() throws SQLException {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = getParameterSets();
        mockExecutionResults(parameterSets, ExecuteStatus.COMPENSATING);
        shardingSQLTransport.with(dataSourceName, sql, parameterSets);
        verify(transactionResource).getConnectionMap();
        verify(statement, times(2)).addBatch();
        verify(statement).executeBatch();
    }
    
    private void mockExecutionResults(final List<List<String>> parameterSets, final ExecuteStatus executeStatus) {
        Map<SagaBranchTransaction, ExecuteStatus> executionResults = new ConcurrentHashMap<>();
        executionResults.put(new SagaBranchTransaction(dataSourceName, sql, transferList(parameterSets)), executeStatus);
        when(sagaTransaction.getExecutionResults()).thenReturn(executionResults);
    }
    
    private List<List<Object>> transferList(final List<List<String>> origin) {
        List<List<Object>> result = Lists.newArrayList();
        for (List<String> each : origin) {
            result.add(Lists.<Object>newArrayList(each));
        }
        return result;
    }
    
    @Test
    public void assertWithNoResultForMultiParametersSuccess() throws SQLException {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = getParameterSets();
        assertThat(shardingSQLTransport.with(dataSourceName, sql, parameterSets), instanceOf(JsonSuccessfulSagaResponse.class));
        verify(statement, times(2)).addBatch();
        verify(statement).executeBatch();
    }
    
    @Test(expected = TransportFailedException.class)
    public void assertWithNoResultForMultiParametersFailure() throws SQLException {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        when(statement.executeBatch()).thenThrow(new SQLException("test execute failed"));
        List<List<String>> parameterSets = getParameterSets();
        assertThat(shardingSQLTransport.with(dataSourceName, sql, parameterSets), instanceOf(JsonSuccessfulSagaResponse.class));
        verify(statement, times(2)).addBatch();
    }
    
    private List<List<String>> getParameterSets() {
        List<List<String>> result = Lists.newArrayList();
        List<String> parameters = Lists.newArrayList();
        parameters.add("1");
        parameters.add("x");
        result.add(parameters);
        parameters = Lists.newArrayList();
        parameters.add("2");
        parameters.add("y");
        result.add(parameters);
        return result;
    }
    
    @Test
    public void assertWithNoResultForEmptyParameters() throws SQLException {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = Lists.newArrayList();
        assertThat(shardingSQLTransport.with(dataSourceName, sql, parameterSets), instanceOf(JsonSuccessfulSagaResponse.class));
        verify(statement).executeUpdate();
    }
    
    @Test
    public void assertWithEmptySQL() throws SQLException {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = Lists.newArrayList();
        assertThat(shardingSQLTransport.with(dataSourceName, "", parameterSets).body(), is("Skip empty transaction/compensation"));
        assertThat(shardingSQLTransport.with(dataSourceName, null, parameterSets).body(), is("Skip empty transaction/compensation"));
    }
    
    @Test
    public void assertWithRollbackTag() {
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = Lists.newArrayList();
        try {
            shardingSQLTransport.with(dataSourceName, SagaDefinitionBuilder.ROLLBACK_TAG, parameterSets);
        } catch (TransportFailedException ex) {
            assertThat(ex.getMessage(), is("Forced Rollback tag has been checked, saga will rollback this transaction"));
        }
    }
    
    @Test(expected = TransportFailedException.class)
    public void assertGetConnectionFailure() throws SQLException {
        ConcurrentMap<String, Connection> connectionMap = new ConcurrentHashMap<>();
        Connection connection = mock(Connection.class);
        connectionMap.put(dataSourceName, connection);
        when(transactionResource.getConnectionMap()).thenReturn(connectionMap);
        when(connection.getAutoCommit()).thenThrow(new SQLException("test get autocommit fail"));
        ShardingSQLTransport shardingSQLTransport = new ShardingSQLTransport(sagaTransaction);
        List<List<String>> parameterSets = Lists.newArrayList();
        shardingSQLTransport.with(dataSourceName, sql, parameterSets);
    }
}
