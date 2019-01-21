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

package io.shardingsphere.transaction.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public final class SagaTransactionTest {
    
    private SagaTransaction sagaTransaction;
    
    @Mock
    private SagaPersistence persistence;
    
    @Before
    public void setUp() {
        sagaTransaction = new SagaTransaction(new SagaConfiguration(), persistence);
    }
    
    @Test
    public void assertNextBranchTransactionGroup() {
        sagaTransaction.nextBranchTransactionGroup();
        assertNotNull(sagaTransaction.getCurrentBranchTransactionGroup());
        assertThat(sagaTransaction.getBranchTransactionGroups().size(), is(1));
        sagaTransaction.nextBranchTransactionGroup();
        assertThat(sagaTransaction.getBranchTransactionGroups().size(), is(2));
    }
    
    @Test
    public void assertSaveNewSnapshot() {
        sagaTransaction.nextBranchTransactionGroup();
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.saveNewSnapshot(sagaBranchTransaction);
        verify(persistence, never()).persistSnapshot(ArgumentMatchers.<SagaSnapshot>any());
        sagaTransaction.getSagaConfiguration().setRecoveryPolicy(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY);
        sagaTransaction.saveNewSnapshot(sagaBranchTransaction);
        verify(persistence).persistSnapshot(ArgumentMatchers.<SagaSnapshot>any());
    }
    
    @Test
    public void assertUpdateSnapshot() {
        sagaTransaction.nextBranchTransactionGroup();
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.updateSnapshot(sagaBranchTransaction, ExecuteStatus.SUCCESS);
        verify(persistence, never()).updateSnapshotStatus(ArgumentMatchers.<String>any(), eq(sagaBranchTransaction.hashCode()), eq(ExecuteStatus.SUCCESS));
        sagaTransaction.getSagaConfiguration().setRecoveryPolicy(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY);
        sagaTransaction.updateSnapshot(sagaBranchTransaction, ExecuteStatus.SUCCESS);
        verify(persistence).updateSnapshotStatus(ArgumentMatchers.<String>any(), eq(sagaBranchTransaction.hashCode()), eq(ExecuteStatus.SUCCESS));
    }
    
    @Test
    public void assertUpdateExecutionResultWithContainsException() {
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.FAILURE);
        assertThat(sagaTransaction.getExecutionResults().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResults().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResults().get(sagaBranchTransaction), is(ExecuteStatus.FAILURE));
        assertTrue(sagaTransaction.isContainsException());
    }
    
    @Test
    public void assertUpdateExecutionResultWithoutContainsException() {
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.EXECUTING);
        assertThat(sagaTransaction.getExecutionResults().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResults().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResults().get(sagaBranchTransaction), is(ExecuteStatus.EXECUTING));
        assertFalse(sagaTransaction.isContainsException());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void assertGetSagaDefinitionBuilder() throws IOException {
        sagaTransaction.nextBranchTransactionGroup();
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.saveNewSnapshot(sagaBranchTransaction);
        sagaTransaction.updateSnapshot(sagaBranchTransaction, ExecuteStatus.SUCCESS);
        SagaDefinitionBuilder builder = sagaTransaction.getSagaDefinitionBuilder();
        ObjectMapper jacksonObjectMapper = new ObjectMapper();
        Map sagaDefinitionMap = jacksonObjectMapper.readValue(builder.build(), Map.class);
        assertThat(sagaDefinitionMap.get("policy").toString(), is(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY));
        assertThat(sagaDefinitionMap.get("requests"), instanceOf(List.class));
        List<Object> requests = (List<Object>) sagaDefinitionMap.get("requests");
        assertThat(requests.size(), is(1));
        assertThat(requests.get(0), instanceOf(Map.class));
        Map<String, Object> request = (Map<String, Object>) requests.get(0);
        assertThat(request.size(), is(7));
        assertTrue(request.containsKey("id"));
        assertTrue(request.containsKey("datasource"));
        assertTrue(request.containsKey("type"));
        assertTrue(request.containsKey("transaction"));
        assertTrue(request.containsKey("compensation"));
        assertTrue(request.containsKey("parents"));
        assertTrue(request.containsKey("failRetryDelayMilliseconds"));
    }
    
    @Test
    public void assertCleanSnapshot() {
        sagaTransaction.cleanSnapshot();
        verify(persistence).cleanSnapshot(ArgumentMatchers.<String>any());
    }
}
