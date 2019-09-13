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

package io.shardingsphere.transaction.base.hook;

import io.shardingsphere.transaction.base.context.SQLTransaction;
import io.shardingsphere.transaction.base.context.ExecuteStatus;
import io.shardingsphere.transaction.base.context.LogicSQLTransaction;
import io.shardingsphere.transaction.base.context.TransactionContext;
import io.shardingsphere.transaction.base.hook.revert.utils.MockTestUtil;
import io.shardingsphere.transaction.base.saga.SagaShardingTransactionManager;
import lombok.SneakyThrows;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.optimize.sharding.statement.ShardingOptimizedStatement;
import org.apache.shardingsphere.core.optimize.sharding.statement.ShardingTransparentOptimizedStatement;
import org.apache.shardingsphere.core.parse.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.spi.database.DataSourceMetaData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SQLTransactionExecutionHookTest {
    
    @Mock
    private TransactionContext transactionContext;
    
    @Mock
    private DataSourceMetaData dataSourceMetaData;
    
    @Mock
    private LogicSQLTransaction logicSQLTransaction;
    
    @Mock
    private SQLTransaction sqlTransaction;
    
    private Map<String, Object> shardingExecuteDataMap = new LinkedHashMap<>();
    
    private SQLTransactionExecutionHook sqlExecutionHook = new SQLTransactionExecutionHook();
    
    private Map<String, Connection> cachedConnections = new HashMap<>();
    
    
    @Before
    public void setUp() {
        when(transactionContext.getCurrentLogicSQLTransaction()).thenReturn(logicSQLTransaction);
        TableMetaData tableMetaData = MockTestUtil.mockTableMetaData("c1", "c2");
        MockTestUtil.addPrimaryKeyColumn(tableMetaData, "pk1");
        when(logicSQLTransaction.getTableMetaData()).thenReturn(tableMetaData);
    }
    
    @Test
    public void assertStartWithinTransaction() throws SQLException {
        when(logicSQLTransaction.isWritableTransaction()).thenReturn(true);
        shardingExecuteDataMap.put(SagaShardingTransactionManager.SAGA_TRANSACTION_KEY, transactionContext);
        cachedConnections.put("ds", MockTestUtil.mockConnection());
        when(transactionContext.getCachedConnections()).thenReturn(cachedConnections);
        SQLStatement sqlStatement = MockTestUtil.mockDeleteStatement("t_order");
        ShardingOptimizedStatement optimizedStatement = new ShardingTransparentOptimizedStatement(sqlStatement);
        when(logicSQLTransaction.getSqlRouteResult()).thenReturn(MockTestUtil.mockSQLRouteResult(optimizedStatement, "ds", "t_order", "t_order_0"));
        RouteUnit routeUnit = MockTestUtil.mockRouteUnit("ds", "delete from t_order_0 where c1=? and c2=? and c3=?", Arrays.<Object>asList(1, 2, 3));
        sqlExecutionHook.start(routeUnit, dataSourceMetaData, true, shardingExecuteDataMap);
        verify(transactionContext).addSQLTransaction(any(SQLTransaction.class));
    }
    
    @Test
    public void assertStartWithoutTransaction() {
        sqlExecutionHook.start(mock(RouteUnit.class), dataSourceMetaData, true, shardingExecuteDataMap);
        verify(transactionContext, never()).addSQLTransaction(any(SQLTransaction.class));
    }
    
    @Test
    public void assertStartIsNotDMLLogicSQL() {
        sqlExecutionHook.start(mock(RouteUnit.class), dataSourceMetaData, true, shardingExecuteDataMap);
        verify(transactionContext, never()).addSQLTransaction(any(SQLTransaction.class));
    }
    
    @Test
    public void assertFinishSuccess() {
        setBranchTransaction();
        sqlExecutionHook.finishSuccess();
        verify(sqlTransaction).setExecuteStatus(ExecuteStatus.SUCCESS);
    }
    
    @Test
    public void assertFinishFailure() {
        setBranchTransaction();
        sqlExecutionHook.finishFailure(mock(Exception.class));
        verify(sqlTransaction).setExecuteStatus(ExecuteStatus.FAILURE);
    }
    
    @SneakyThrows
    private void setBranchTransaction() {
        Field field = sqlExecutionHook.getClass().getDeclaredField("sqlTransaction");
        field.setAccessible(true);
        field.set(sqlExecutionHook, sqlTransaction);
    }
}