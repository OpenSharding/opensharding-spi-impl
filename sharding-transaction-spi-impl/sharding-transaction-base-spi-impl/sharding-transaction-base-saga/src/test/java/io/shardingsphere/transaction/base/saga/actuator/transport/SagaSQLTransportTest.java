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

package io.shardingsphere.transaction.base.saga.actuator.transport;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.base.context.BranchTransaction;
import io.shardingsphere.transaction.base.context.ExecuteStatus;
import io.shardingsphere.transaction.base.context.TransactionContext;
import io.shardingsphere.transaction.base.saga.actuator.definition.SagaDefinitionFactory;
import org.apache.servicecomb.saga.core.TransportFailedException;
import org.apache.shardingsphere.transaction.core.TransactionOperationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SagaSQLTransportTest {
    
    @Mock
    private TransactionContext transactionContext;
    
    @Mock
    private BranchTransaction branchTransaction;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    private SagaSQLTransport sagaSQLTransport;
    
    private final Map<String, Connection> cachedConnections = new HashMap<>();
    
    @Before
    public void setUp() throws SQLException {
        sagaSQLTransport = new SagaSQLTransport(transactionContext);
        when(transactionContext.getCachedConnections()).thenReturn(cachedConnections);
        cachedConnections.put("ds1", connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    }
    
    @Test(expected = TransportFailedException.class)
    public void assertTransportWithRollback() {
        try {
            sagaSQLTransport.with("ds1", SagaDefinitionFactory.ROLLBACK_TAG, Lists.<List<String>>newLinkedList());
        } catch (final TransportFailedException ex) {
            verify(transactionContext).changeAllLogicTransactionStatus(ExecuteStatus.COMPENSATING);
            throw ex;
        }
    }
    
    @Test
    public void assertWithBranchTransactionNotPresent() {
        when(transactionContext.findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList())).thenReturn(Optional.<BranchTransaction>absent());
        sagaSQLTransport.with("ds1", "xxx", Lists.<List<String>>newLinkedList());
        verify(transactionContext).findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList());
    }
    
    @Test
    public void assertWithExecuteStatusSuccess() throws SQLException {
        when(branchTransaction.getExecuteStatus()).thenReturn(ExecuteStatus.SUCCESS);
        when(transactionContext.findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList())).thenReturn(Optional.of(branchTransaction));
        sagaSQLTransport.with("ds1", "xxx", Lists.<List<String>>newLinkedList());
        verify(connection, never()).prepareStatement("xxx");
    }
    
    @Test
    public void assertWithExecuteStatusFailedOfRollback() throws SQLException {
        when(branchTransaction.getExecuteStatus()).thenReturn(ExecuteStatus.FAILURE);
        when(transactionContext.getOperationType()).thenReturn(TransactionOperationType.ROLLBACK);
        when(transactionContext.findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList())).thenReturn(Optional.of(branchTransaction));
        when(transactionContext.getCachedConnections()).thenReturn(cachedConnections);
        sagaSQLTransport.with("ds1", "xxx", Lists.<List<String>>newLinkedList());
        verify(connection, never()).prepareStatement("xxx");
    }
    
    @Test
    public void assertWithExecuteSQL() throws SQLException {
        when(branchTransaction.getExecuteStatus()).thenReturn(ExecuteStatus.COMPENSATING);
        when(transactionContext.findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList())).thenReturn(Optional.of(branchTransaction));
        sagaSQLTransport.with("ds1", "xxx", Lists.<List<String>>newLinkedList());
        verify(connection).prepareStatement("xxx");
        verify(preparedStatement).executeUpdate();
    }
    
    @Test
    public void assertWithExecuteBatchSQL() throws SQLException {
        when(branchTransaction.getExecuteStatus()).thenReturn(ExecuteStatus.COMPENSATING);
        when(transactionContext.findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList())).thenReturn(Optional.of(branchTransaction));
        List<List<String>> parameters = Lists.newLinkedList();
        parameters.add(Arrays.asList("1", "2", "3"));
        sagaSQLTransport.with("ds1", "xxx", parameters);
        verify(connection).prepareStatement("xxx");
        verify(preparedStatement).executeBatch();
    }
}