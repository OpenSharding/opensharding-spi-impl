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

package io.shardingsphere.transaction.base.saga.actuator.definition;

import io.shardingsphere.transaction.base.context.SQLTransaction;
import io.shardingsphere.transaction.base.context.LogicSQLTransaction;
import io.shardingsphere.transaction.base.context.TransactionContext;
import io.shardingsphere.transaction.base.hook.revert.RevertSQLResult;
import io.shardingsphere.transaction.base.saga.config.SagaConfiguration;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SagaDefinitionFactoryTest {
    
    private TransactionContext transactionContext = new TransactionContext();
    
    private SagaConfiguration sagaConfiguration = new SagaConfiguration();
    
    @Before
    public void setUp() {
    }
    
    @Test
    public void assertNewInstanceOfBackwardRecovery() {
        transactionContext.getLogicSQLTransactions().addAll(mockLogicSQLTransactions(3, 2));
        SagaDefinition actual = SagaDefinitionFactory.newInstance(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY, sagaConfiguration, transactionContext);
        assertThat(actual.getPolicy(), is(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY));
        assertThat(actual.getRequests().size(), is(7));
        int i = 0;
        for (SagaRequest each : actual.getRequests()) {
            if (i <= 1) {
                assertThat(each.getParents().size(), is(0));
            } else {
                assertThat(each.getParents().size(), is(2));
            }
            if (i <= 5) {
                assertThat(each.getDatasource(), is("ds"));
                assertThat(each.getTransaction().getSql(), is("tx-sql"));
                assertThat(each.getTransaction().getParams().size(), is(3));
                assertThat(each.getId(), instanceOf(String.class));
                assertThat(each.getType(), is("sql"));
                assertThat(each.getFailRetryDelayMilliseconds(), is(sagaConfiguration.getTransactionRetryDelayMilliseconds()));
                assertThat(each.getCompensation().getSql(), is("revert-sql"));
                assertThat(each.getCompensation().getParams().size(), is(3));
                assertThat(each.getCompensation().getRetries(), is(sagaConfiguration.getCompensationMaxRetries()));
            } else {
                assertThat(each.getDatasource(), is("rollbackTag"));
                assertThat(each.getTransaction().getSql(), is("rollbackTag"));
                assertThat(each.getTransaction().getParams().size(), is(0));
                assertThat(each.getCompensation().getSql(), is("rollbackTag"));
                assertThat(each.getCompensation().getParams().size(), is(0));
            }
            i++;
        }
    }
    
    @Test
    public void assertNewInstanceOfForwardRecovery() {
        transactionContext.getLogicSQLTransactions().addAll(mockLogicSQLTransactions(2, 4));
        SagaDefinition actual = SagaDefinitionFactory.newInstance(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY, sagaConfiguration, transactionContext);
        assertThat(actual.getPolicy(), is(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY));
        assertThat(actual.getRequests().size(), is(8));
        for (SagaRequest each : actual.getRequests()) {
            assertThat(each.getDatasource(), is("ds"));
            assertThat(each.getTransaction().getSql(), is("tx-sql"));
            assertThat(each.getTransaction().getParams().size(), is(3));
            assertThat(each.getCompensation().getSql(), is("revert-sql"));
            assertThat(each.getCompensation().getParams().size(), is(3));
        }
    }
    
    private List<LogicSQLTransaction> mockLogicSQLTransactions(final int logicSQLCount, int branchCount) {
        List<LogicSQLTransaction> result = new LinkedList<>();
        for (int i = 0; i < logicSQLCount; i++) {
            LogicSQLTransaction logicSQLTransaction = mock(LogicSQLTransaction.class);
            when(logicSQLTransaction.getSqlTransactions()).thenReturn(mockBranchTransactions(branchCount));
            result.add(logicSQLTransaction);
        }
        return result;
    }
    
    private Queue<SQLTransaction> mockBranchTransactions(final int count) {
        final Queue<SQLTransaction> result = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < count; i++) {
            SQLTransaction sqlTransaction = new SQLTransaction("ds", "tx-sql", mockParameters());
            RevertSQLResult revertSQLResult = new RevertSQLResult("revert-sql");
            revertSQLResult.getParameters().addAll(mockParameters());
            sqlTransaction.setRevertSQLResult(revertSQLResult);
            result.offer(sqlTransaction);
        }
        return result;
    }
    
    private List<Collection<Object>> mockParameters() {
        List<Collection<Object>> result = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            result.add(Arrays.<Object>asList(1, 2, 3));
        }
        return result;
    }
}