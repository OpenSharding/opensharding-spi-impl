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

package io.shardingsphere.transaction.base.context;

import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.optimize.api.segment.Tables;
import org.apache.shardingsphere.core.optimize.sharding.statement.ShardingOptimizedStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class SagaTransactionTest {
    
    private TransactionContext sagaTransaction;

    @Mock
    private ShardingOptimizedStatement optimizedStatement;
    
    @Mock
    private InsertStatement sqlStatement;
    
    @Mock
    private Tables tables;
    
    @Mock
    private SQLRouteResult sqlRouteResult;
    
    @Mock
    private TableMetas tableMetas;
    
    private final String actualSQL = "UPDATE";
    
    @Before
    public void setUp() {
        sagaTransaction = new TransactionContext();
        when(sqlRouteResult.getShardingStatement()).thenReturn(optimizedStatement);
        when(optimizedStatement.getSQLStatement()).thenReturn(sqlStatement);
        when(optimizedStatement.getTables()).thenReturn(tables);
        when(tables.getSingleTableName()).thenReturn("t_order");
    }
    
    @Test
    public void assertNextLogicSQLTransaction() {
        sagaTransaction.nextLogicSQLTransaction("sql1");
        sagaTransaction.initLogicSQLTransaction(sqlRouteResult, tableMetas);
        assertThat(sagaTransaction.getCurrentLogicSQLTransaction(), instanceOf(LogicSQLTransaction.class));
        assertThat(sagaTransaction.getLogicSQLTransactions().size(), is(1));
        sagaTransaction.nextLogicSQLTransaction("sql2");
        sagaTransaction.initLogicSQLTransaction(sqlRouteResult, tableMetas);
        assertThat(sagaTransaction.getLogicSQLTransactions().size(), is(2));
    }
    
    @Test
    public void assertAddBranchTransactionWithFailureStatus() {
        sagaTransaction.nextLogicSQLTransaction("sql1");
        sagaTransaction.initLogicSQLTransaction(sqlRouteResult, tableMetas);
        sagaTransaction.addSQLTransaction(new SQLTransaction("", actualSQL, null, ExecuteStatus.FAILURE));
        assertThat(sagaTransaction.getCurrentLogicSQLTransaction().getSqlTransactions().size(), is(1));
        assertTrue(sagaTransaction.isContainsException());
    }
    
    @Test
    public void assertAddBranchTransactionWithSuccessStatus() {
        sagaTransaction.nextLogicSQLTransaction("sql1");
        sagaTransaction.initLogicSQLTransaction(sqlRouteResult, tableMetas);
        sagaTransaction.addSQLTransaction(new SQLTransaction("", actualSQL, null, ExecuteStatus.SUCCESS));
        assertThat(sagaTransaction.getCurrentLogicSQLTransaction().getSqlTransactions().size(), is(1));
        assertFalse(sagaTransaction.isContainsException());
    }
}
