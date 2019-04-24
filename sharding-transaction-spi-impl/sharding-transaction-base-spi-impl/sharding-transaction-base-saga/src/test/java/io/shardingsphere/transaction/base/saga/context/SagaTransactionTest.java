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

package io.shardingsphere.transaction.base.saga.context;

import io.shardingsphere.transaction.base.context.ExecuteStatus;
import io.shardingsphere.transaction.base.context.BranchTransaction;
import io.shardingsphere.transaction.base.context.LogicSQLTransaction;
import io.shardingsphere.transaction.base.context.TransactionContext;
import org.apache.shardingsphere.core.constant.SQLType;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
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
    private DMLStatement sqlStatement;
    
    @Mock
    private Tables tables;
    
    @Mock
    private SQLRouteResult sqlRouteResult;
    
    @Mock
    private ShardingTableMetaData shardingTableMetaData;
    
    private final String actualSQL = "UPDATE";
    
    @Before
    public void setUp() {
        sagaTransaction = new TransactionContext();
        when(sqlStatement.getType()).thenReturn(SQLType.DML);
        when(sqlRouteResult.getSqlStatement()).thenReturn(sqlStatement);
        when(sqlStatement.getTables()).thenReturn(tables);
        when(tables.getSingleTableName()).thenReturn("t_order");
    }
    
    @Test
    public void assertNextLogicSQLTransaction() {
        sagaTransaction.nextLogicSQLTransaction(sqlRouteResult, shardingTableMetaData);
        assertThat(sagaTransaction.getCurrentLogicSQLTransaction(), instanceOf(LogicSQLTransaction.class));
        assertThat(sagaTransaction.getLogicSQLTransactions().size(), is(1));
        sagaTransaction.nextLogicSQLTransaction(sqlRouteResult, shardingTableMetaData);
        assertThat(sagaTransaction.getLogicSQLTransactions().size(), is(2));
    }
    
    @Test
    public void assertAddBranchTransactionWithFailureStatus() {
        sagaTransaction.nextLogicSQLTransaction(sqlRouteResult, shardingTableMetaData);
        sagaTransaction.addBranchTransaction(new BranchTransaction("", actualSQL, null, ExecuteStatus.FAILURE));
        assertThat(sagaTransaction.getCurrentLogicSQLTransaction().getBranchTransactions().size(), is(1));
        assertTrue(sagaTransaction.isContainsException());
    }
    
    @Test
    public void assertAddBranchTransactionWithSuccessStatus() {
        sagaTransaction.nextLogicSQLTransaction(sqlRouteResult, shardingTableMetaData);
        sagaTransaction.addBranchTransaction(new BranchTransaction("", actualSQL, null, ExecuteStatus.SUCCESS));
        assertThat(sagaTransaction.getCurrentLogicSQLTransaction().getBranchTransactions().size(), is(1));
        assertFalse(sagaTransaction.isContainsException());
    }
}
