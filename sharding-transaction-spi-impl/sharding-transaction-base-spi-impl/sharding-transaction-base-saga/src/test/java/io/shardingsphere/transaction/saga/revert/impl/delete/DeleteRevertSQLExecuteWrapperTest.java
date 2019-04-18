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

package io.shardingsphere.transaction.saga.revert.impl.delete;

import io.shardingsphere.transaction.saga.revert.api.DMLSnapshotAccessor;
import io.shardingsphere.transaction.saga.revert.api.SnapshotSQLStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeleteRevertSQLExecuteWrapperTest {
    
    @Mock
    private DMLSnapshotAccessor snapshotAccessor;
    
    @Mock
    private SnapshotSQLStatement snapshotSQLStatement;
    
    private DeleteRevertSQLExecuteWrapper deleteRevertSQLExecuteWrapper;
    
    @Before
    public void setUp() {
        when(snapshotSQLStatement.getActualTableName()).thenReturn("t_order");
        when(snapshotAccessor.getSnapshotSQLStatement()).thenReturn(snapshotSQLStatement);
        deleteRevertSQLExecuteWrapper = new DeleteRevertSQLExecuteWrapper(snapshotAccessor);
    }
    
    @Test
    public void assertCreateRevertSQLContext() throws SQLException {
        DeleteRevertSQLContext actual = deleteRevertSQLExecuteWrapper.createRevertSQLContext();
        assertThat(actual.getActualTable(), is("t_order"));
        verify(snapshotAccessor).queryUndoData();
    }
    
    @Test
    public void assertGenerateRevertSQL() {
    }

}
