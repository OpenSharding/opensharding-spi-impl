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

import io.shardingsphere.transaction.saga.context.SagaTransactionHolder;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public final class SagaSQLShardHookTest {
    
    @Mock
    private SagaTransaction sagaTransaction;
    
    @Mock
    private SQLRouteResult sqlRouteResult;
    
    @Mock
    private ShardingTableMetaData shardingTableMetaData;
    
    
    private final SagaSQLShardHook sagaSQLShardHook = new SagaSQLShardHook();
    
    @Before
    public void setUp() {
        SagaTransactionHolder.set(sagaTransaction);
    }
    
    @After
    public void tearDown() {
        SagaTransactionHolder.clear();
    }
    
    @Test
    public void assertFinishSuccess() {
        sagaSQLShardHook.start("logicSQL");
        sagaSQLShardHook.finishSuccess(sqlRouteResult, shardingTableMetaData);
        verify(sagaTransaction).nextLogicSQLTransaction(sqlRouteResult, shardingTableMetaData);
    }
}
