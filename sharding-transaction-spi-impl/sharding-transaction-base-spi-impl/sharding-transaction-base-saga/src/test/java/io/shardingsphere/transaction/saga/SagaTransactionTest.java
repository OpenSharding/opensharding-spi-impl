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

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.constant.SQLType;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parsing.parser.context.table.Tables;
import org.apache.shardingsphere.core.parsing.parser.sql.dml.DMLStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import lombok.SneakyThrows;

@RunWith(MockitoJUnitRunner.class)
public final class SagaTransactionTest {
    
    private SagaTransaction sagaTransaction;
    
    @Mock
    private SagaPersistence persistence;
    
    @Mock
    private DMLStatement sqlStatement;
    
    @Mock
    private ShardingTableMetaData shardingTableMetaData;
    
    private final String sql = "UPDATE";
    
    @Before
    public void setUp() {
        sagaTransaction = new SagaTransaction(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY, persistence);
        when(sqlStatement.getType()).thenReturn(SQLType.DML);
    }
    
    @Test
    public void assertNextBranchTransactionGroup() {
        sagaTransaction.nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
        assertNotNull(sagaTransaction.getCurrentBranchTransactionGroup());
        assertThat(sagaTransaction.getBranchTransactionGroups().size(), is(1));
        sagaTransaction.nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
        assertThat(sagaTransaction.getBranchTransactionGroups().size(), is(2));
    }
    
    @Test
    @SneakyThrows
    public void assertSaveNewSnapshot() {
        when(sqlStatement.getTables()).thenReturn(mock(Tables.class));
        sagaTransaction.nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        when(sagaBranchTransaction.getDataSourceName()).thenReturn("ds");
        when(sagaBranchTransaction.getParameterSets()).thenReturn(Collections.<List<Object>>emptyList());
        sagaTransaction.saveNewSnapshot(sagaBranchTransaction);
        verify(persistence, never()).persistSnapshot(ArgumentMatchers.<SagaSnapshot>any());
        Field recoveryPolicyField = SagaTransaction.class.getDeclaredField("recoveryPolicy");
        recoveryPolicyField.setAccessible(true);
        recoveryPolicyField.set(sagaTransaction, RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY);
        sagaTransaction.saveNewSnapshot(sagaBranchTransaction);
        verify(persistence).persistSnapshot(ArgumentMatchers.<SagaSnapshot>any());
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
    
    @Test
    public void assertCleanSnapshot() {
        sagaTransaction.cleanSnapshot();
        verify(persistence).cleanSnapshot(ArgumentMatchers.<String>any());
    }
}
