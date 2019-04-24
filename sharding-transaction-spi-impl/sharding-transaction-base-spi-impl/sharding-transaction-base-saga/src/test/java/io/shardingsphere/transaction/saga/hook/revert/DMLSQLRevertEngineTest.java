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

package io.shardingsphere.transaction.saga.hook.revert;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.hook.revert.executor.SQLRevertExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DMLSQLRevertEngineTest {
    
    @Mock
    private SQLRevertExecutor sqlRevertExecutor;
    
    private DMLSQLRevertEngine sqlRevertEngine;
    
    @Before
    public void setUp() {
        sqlRevertEngine = new DMLSQLRevertEngine(sqlRevertExecutor);
    }
    
    @Test
    public void assertRevertSQLNotExist() {
        when(sqlRevertExecutor.revertSQL()).thenReturn(Optional.<String>absent());
        Optional<RevertSQLResult> actual = sqlRevertEngine.revert();
        assertFalse(actual.isPresent());
    }
    
    @Test
    public void assertRevertSQLExist() {
        when(sqlRevertExecutor.revertSQL()).thenReturn(Optional.of("revert sql"));
        Optional<RevertSQLResult> actual = sqlRevertEngine.revert();
        assertTrue(actual.isPresent());
        verify(sqlRevertExecutor).fillParameters(any(RevertSQLResult.class));
    }
}