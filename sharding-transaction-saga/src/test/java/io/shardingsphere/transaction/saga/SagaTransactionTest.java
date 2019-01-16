/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.impl.EmptySagaPersistence;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class SagaTransactionTest {
    
    private SagaTransaction sagaTransaction;
    
    @Before
    public void setUp() {
        sagaTransaction = new SagaTransaction(new SagaConfiguration(), new EmptySagaPersistence());
    }
    
    @Test
    public void assertNextLogicSQL() {
        sagaTransaction.nextBranchTransactionGroup();
        assertNotNull(sagaTransaction.getSagaBranchTransactionGroup());
        assertThat(sagaTransaction.getSagaBranchTransactionGroups().size(), is(1));
        sagaTransaction.nextBranchTransactionGroup();
        assertThat(sagaTransaction.getSagaBranchTransactionGroups().size(), is(2));
    }
    
    @Test
    public void assertRecordResult() {
        sagaTransaction.nextBranchTransactionGroup();
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.recordStart(sagaBranchTransaction);
        assertThat(sagaTransaction.getExecutionResultMap().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResultMap().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResultMap().get(sagaBranchTransaction), is(ExecuteStatus.EXECUTING));
        assertFalse(sagaTransaction.isContainException());
        assertThat(sagaTransaction.getSagaBranchTransactionGroup().size(), is(1));
        sagaTransaction.recordResult(sagaBranchTransaction, ExecuteStatus.SUCCESS);
        assertThat(sagaTransaction.getExecutionResultMap().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResultMap().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResultMap().get(sagaBranchTransaction), is(ExecuteStatus.SUCCESS));
        assertFalse(sagaTransaction.isContainException());
        assertThat(sagaTransaction.getSagaBranchTransactionGroup().size(), is(1));
        sagaTransaction.recordResult(sagaBranchTransaction, ExecuteStatus.FAILURE);
        assertThat(sagaTransaction.getExecutionResultMap().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResultMap().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResultMap().get(sagaBranchTransaction), is(ExecuteStatus.FAILURE));
        assertTrue(sagaTransaction.isContainException());
        assertThat(sagaTransaction.getSagaBranchTransactionGroup().size(), is(1));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void assertGetSagaDefinitionBuilder() throws IOException {
        sagaTransaction.nextBranchTransactionGroup();
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.recordStart(sagaBranchTransaction);
        sagaTransaction.recordResult(sagaBranchTransaction, ExecuteStatus.SUCCESS);
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
}
