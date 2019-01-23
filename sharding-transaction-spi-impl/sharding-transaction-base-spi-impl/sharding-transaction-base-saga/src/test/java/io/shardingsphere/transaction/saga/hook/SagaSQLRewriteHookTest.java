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

import io.shardingsphere.transaction.saga.SagaTransaction;
import lombok.SneakyThrows;
import org.apache.shardingsphere.core.routing.SQLUnit;
import org.apache.shardingsphere.core.routing.type.TableUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SagaSQLRewriteHookTest {
    
    private final SagaSQLRewriteHook sagaSQLRewriteHook = new SagaSQLRewriteHook();
    
    @Mock
    private SagaTransaction sagaTransaction;
    
    @Before
    @SneakyThrows
    public void setUp() {
        Field sagaTransactionField = SagaSQLRewriteHook.class.getDeclaredField("sagaTransaction");
        sagaTransactionField.setAccessible(true);
        sagaTransactionField.set(sagaSQLRewriteHook, sagaTransaction);
        Map<SQLUnit, TableUnit> tableUnitMap = new ConcurrentHashMap<>();
        when(sagaTransaction.getTableUnitMap()).thenReturn(tableUnitMap);
    }
    
    @Test
    public void assertFinishSuccess() {
        TableUnit tableUnit = mock(TableUnit.class);
        SQLUnit sqlUnit = mock(SQLUnit.class);
        sagaSQLRewriteHook.start(tableUnit);
        sagaSQLRewriteHook.finishSuccess(sqlUnit);
        assertThat(sagaTransaction.getTableUnitMap().size(), is(1));
        assertThat(sagaTransaction.getTableUnitMap().get(sqlUnit), is(tableUnit));
    }
}
