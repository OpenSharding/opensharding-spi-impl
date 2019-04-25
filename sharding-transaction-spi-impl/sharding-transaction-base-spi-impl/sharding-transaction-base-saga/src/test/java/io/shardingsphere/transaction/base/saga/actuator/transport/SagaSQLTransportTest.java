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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
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
    
    private SagaSQLTransport sagaSQLTransport;
    
    @Before
    public void setUp() {
        sagaSQLTransport = new SagaSQLTransport(transactionContext);
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
        sagaSQLTransport.with("ds1", "sql", Lists.<List<String>>newLinkedList());
        verify(transactionContext).findBranchTransaction(anyString(), anyString(), ArgumentMatchers.<List<String>>anyList());
    }
}