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

package io.shardingsphere.transaction.saga.hook;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.SagaShardingTransactionManager;
import io.shardingsphere.transaction.saga.SagaTransaction;
import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import org.apache.shardingsphere.core.executor.ShardingExecuteDataMap;
import org.apache.shardingsphere.core.executor.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.core.routing.RouteUnit;
import org.apache.shardingsphere.core.routing.SQLUnit;
import org.apache.shardingsphere.core.routing.type.RoutingTable;
import org.apache.shardingsphere.core.routing.type.TableUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class SagaSQLExecutionHookTest {
    
    private final SagaSQLExecutionHook sagaSQLExecutionHook = new SagaSQLExecutionHook();
    
    private final RouteUnit routeUnit = new RouteUnit("", new SQLUnit("", Lists.<List<Object>>newArrayList()));
    
    @Mock
    private SagaTransaction sagaTransaction;
    
    @Before
    public void setUp() {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY, sagaTransaction);
        Map<SQLUnit, TableUnit> tableUnitMap = new ConcurrentHashMap<>();
        TableUnit tableUnit = new TableUnit("");
        tableUnit.getRoutingTables().add(mock(RoutingTable.class));
        tableUnitMap.put(routeUnit.getSqlUnit(), tableUnit);
        when(sagaTransaction.isDMLBranchTransactionGroup()).thenReturn(true);
        when(sagaTransaction.getTableUnitMap()).thenReturn(tableUnitMap);
        ShardingExecuteDataMap.setDataMap(dataMap);
    }
    
    @Test
    public void assertStart() {
        sagaSQLExecutionHook.start(routeUnit, null, true, ShardingExecuteDataMap.getDataMap());
        verify(sagaTransaction).saveNewSnapshot(any(SagaBranchTransaction.class));
    }
    
    @Test
    public void assertFinishSuccess() {
        SagaBranchTransaction branchTransaction = new SagaBranchTransaction(routeUnit.getDataSourceName(), routeUnit.getSqlUnit().getSql(), routeUnit.getSqlUnit().getParameterSets());
        sagaSQLExecutionHook.start(routeUnit, null, true, ShardingExecuteDataMap.getDataMap());
        sagaSQLExecutionHook.finishSuccess();
        verify(sagaTransaction).updateExecutionResult(branchTransaction ,ExecuteStatus.SUCCESS);
    }
    
    @Test
    public void assertFinishFailure() {
        when(sagaTransaction.getSagaConfiguration()).thenReturn(new SagaConfiguration());
        sagaSQLExecutionHook.start(routeUnit, null, true, ShardingExecuteDataMap.getDataMap());
        sagaSQLExecutionHook.finishFailure(new RuntimeException());
        assertFalse(ExecutorExceptionHandler.isExceptionThrown());
    }
}
