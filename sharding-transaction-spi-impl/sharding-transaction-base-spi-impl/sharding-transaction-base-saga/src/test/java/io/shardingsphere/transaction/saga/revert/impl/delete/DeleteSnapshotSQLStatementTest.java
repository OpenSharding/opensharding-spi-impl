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

import io.shardingsphere.transaction.saga.revert.snapshot.statement.DeleteSnapshotSQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeleteSnapshotSQLStatementTest {
    
    @Mock
    private DeleteStatement deleteStatement;
    
    private DeleteSnapshotSQLStatement snapshotSQLStatement;
    
    private List<Object> actualSQLParameters = new LinkedList<>();
    
    @Before
    public void setUp() {
        snapshotSQLStatement = new DeleteSnapshotSQLStatement("t_order_0", deleteStatement, actualSQLParameters);
        when(deleteStatement.getLogicSQL()).thenReturn("DELETE FROM t_order WHERE order_id = ?");
        when(deleteStatement.getWhereStartIndex()).thenReturn(20);
        when(deleteStatement.getWhereStopIndex()).thenReturn(37);
        when(deleteStatement.getWhereParameterStartIndex()).thenReturn(0);
        when(deleteStatement.getWhereParameterEndIndex()).thenReturn(0);
    }
    
    @Test
    public void assertDeleteSnapshotSQLStatement() {
        actualSQLParameters.add(10);
        assertThat(snapshotSQLStatement.getActualTableName(), is("t_order_0"));
        assertThat(snapshotSQLStatement.getQueryColumnNames(), CoreMatchers.<Collection<String>>is(Collections.singleton("*")));
        assertThat(snapshotSQLStatement.getParameters().size(), is(1));
        assertThat(snapshotSQLStatement.getParameters().iterator().next(), CoreMatchers.<Object>is(10));
        assertThat(snapshotSQLStatement.getWhereClause(), is("WHERE order_id = ?"));
    }
}
