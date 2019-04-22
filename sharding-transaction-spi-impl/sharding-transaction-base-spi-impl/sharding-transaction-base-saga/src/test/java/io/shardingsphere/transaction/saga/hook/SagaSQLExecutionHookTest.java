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
import io.shardingsphere.transaction.saga.SagaShardingTransactionManager;
import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaLogicSQLTransaction;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.resource.SagaTransactionResource;
import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.execute.ShardingExecuteDataMap;
import org.apache.shardingsphere.core.execute.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.SQLUnit;
import org.apache.shardingsphere.core.route.type.RoutingResult;
import org.apache.shardingsphere.core.route.type.RoutingTable;
import org.apache.shardingsphere.core.route.type.TableUnit;
import org.apache.shardingsphere.core.route.type.TableUnits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class SagaSQLExecutionHookTest {
    
    private final SagaSQLExecutionHook sagaSQLExecutionHook = new SagaSQLExecutionHook();
    
//    private final RouteUnit routeUnit = new RouteUnit("", new SQLUnit("UPDATE ?", Lists.newArrayList(new Object(), new Object())));
    
    @Mock
    private SagaTransaction globalTransaction;
    
    @Mock
    private SagaLogicSQLTransaction sagaLogicSQLTransaction;
    
    private TableMetaData tableMetaData;
    
    @Mock
    private RouteUnit routeUnit;
    
    @Mock
    private SQLUnit sqlUnit;
    
    @Mock
    private SQLRouteResult sqlRouteResult;
    
    @Mock
    private DeleteStatement sqlStatement;
    
    @Mock
    private Tables tables;
    
    @Mock
    private RoutingResult routingResult;
    
    @Mock
    private TableUnit tableUnit;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    @Mock
    private ResultSetMetaData resultSetMetaData;
    
    @Mock
    private SagaPersistence sagaPersistence;
    
    @Before
    @SneakyThrows
    public void setUp() {
        SagaTransactionResource transactionResource = mockSagaTransactionResource();
        registerResource(transactionResource);
        mockTableMetaData();
        mockSagaTransaction();
        setShardingExecuteDataMap();
    }
    
    @SneakyThrows
    private SagaTransactionResource mockSagaTransactionResource() {
        return new SagaTransactionResource(sagaPersistence);
    }
    
    @SneakyThrows
    @SuppressWarnings("unchecked")
    private void registerResource(final SagaTransactionResource transactionResource) {
        Field resourceMapField = SagaResourceManager.class.getDeclaredField("TRANSACTION_RESOURCE_MAP");
        resourceMapField.setAccessible(true);
        Map<SagaTransaction, SagaTransactionResource> resourceMap = (Map<SagaTransaction, SagaTransactionResource>) resourceMapField.get(SagaResourceManager.class);
        resourceMap.put(globalTransaction, transactionResource);
        transactionResource.getConnectionMap().putIfAbsent("ds1", connection);
    }
    
    private void mockTableMetaData() {
        Collection<ColumnMetaData> columnMetaDataList = Lists.newLinkedList();
        columnMetaDataList.add(new ColumnMetaData("order_id", "long", true));
        columnMetaDataList.add(new ColumnMetaData("user_id", "long", true));
        columnMetaDataList.add(new ColumnMetaData("status", "string", true));
        tableMetaData = new TableMetaData(columnMetaDataList);
    }
    
    private void mockSagaTransaction() throws SQLException {
        when(routeUnit.getDataSourceName()).thenReturn("ds1");
        when(routeUnit.getSqlUnit()).thenReturn(sqlUnit);
        when(sqlUnit.getSql()).thenReturn("unit sql");
        when(sqlUnit.getParameters()).thenReturn(Lists.newArrayList());
        TableUnits tableUnits = new TableUnits();
        tableUnits.getTableUnits().add(tableUnit);
        when(tableUnit.getDataSourceName()).thenReturn("ds1");
        RoutingTable routingTable = new RoutingTable("t_order", "t_order_0");
        when(tableUnit.getRoutingTables()).thenReturn(Lists.newLinkedList(Collections.singleton(routingTable)));
        when(globalTransaction.getRecoveryPolicy()).thenReturn(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY);
        when(globalTransaction.getCurrentLogicSQLTransaction()).thenReturn(sagaLogicSQLTransaction);
        when(sagaLogicSQLTransaction.getTableMetaData()).thenReturn(tableMetaData);
        when(sagaLogicSQLTransaction.isDMLLogicSQL()).thenReturn(true);
        when(sagaLogicSQLTransaction.getSqlRouteResult()).thenReturn(sqlRouteResult);
        when(sqlRouteResult.getRoutingResult()).thenReturn(routingResult);
        when(routingResult.getTableUnits()).thenReturn(tableUnits);
        when(sqlRouteResult.getSqlStatement()).thenReturn(sqlStatement);
        when(sqlStatement.getTables()).thenReturn(tables);
        when(tables.getSingleTableName()).thenReturn("t_order");
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    }
    
    private void setShardingExecuteDataMap() {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY, globalTransaction);
        ShardingExecuteDataMap.setDataMap(dataMap);
    }
    
    @Test
    public void assertStart() {
        sagaSQLExecutionHook.start(routeUnit, null, true, ShardingExecuteDataMap.getDataMap());
        SagaBranchTransaction branchTransaction = getBranchTransaction();
        assertThat(branchTransaction.getExecuteStatus(), is(ExecuteStatus.EXECUTING));
        verify(sagaPersistence).persistSnapshot(ArgumentMatchers.<SagaSnapshot>any());
    }
    
    @Test
    public void assertFinishSuccess() {
        sagaSQLExecutionHook.start(routeUnit, null, true, ShardingExecuteDataMap.getDataMap());
        sagaSQLExecutionHook.finishSuccess();
        SagaBranchTransaction branchTransaction = getBranchTransaction();
        assertThat(branchTransaction.getExecuteStatus(), is(ExecuteStatus.SUCCESS));
    }
    
    @SneakyThrows
    private SagaBranchTransaction getBranchTransaction() {
        Field field = sagaSQLExecutionHook.getClass().getDeclaredField("branchTransaction");
        field.setAccessible(true);
        return (SagaBranchTransaction) field.get(sagaSQLExecutionHook);
    }
    
    @Test
    public void assertFinishFailure() {
        when(globalTransaction.getRecoveryPolicy()).thenReturn(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY);
        sagaSQLExecutionHook.start(routeUnit, null, true, ShardingExecuteDataMap.getDataMap());
        sagaSQLExecutionHook.finishFailure(new RuntimeException());
        assertFalse(ExecutorExceptionHandler.isExceptionThrown());
    }
    
    @After
    public void tearDown() {
        ShardingExecuteDataMap.getDataMap().remove(SagaShardingTransactionManager.CURRENT_TRANSACTION_KEY);
    }
}
