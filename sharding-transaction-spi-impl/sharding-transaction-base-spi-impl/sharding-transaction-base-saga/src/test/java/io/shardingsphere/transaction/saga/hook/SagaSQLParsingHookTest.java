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

import io.shardingsphere.transaction.saga.context.SagaTransaction;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parsing.parser.sql.SQLStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public final class SagaSQLParsingHookTest {
    
    @Mock
    private SagaTransaction sagaTransaction;
    
    private final SagaSQLParsingHook sagaSQLParsingHook = new SagaSQLParsingHook();
    
    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        Field sagaTransactionField = SagaSQLParsingHook.class.getDeclaredField("sagaTransaction");
        sagaTransactionField.setAccessible(true);
        sagaTransactionField.set(sagaSQLParsingHook, sagaTransaction);
    }
    
    @Test
    public void assertFinishSuccess() {
        String sql = "UPDATE";
        SQLStatement sqlStatement = mock(SQLStatement.class);
        ShardingTableMetaData shardingTableMetaData = mock(ShardingTableMetaData.class);
        sagaSQLParsingHook.start(sql);
        sagaSQLParsingHook.finishSuccess(sqlStatement, shardingTableMetaData);
        verify(sagaTransaction).nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
    }
}
