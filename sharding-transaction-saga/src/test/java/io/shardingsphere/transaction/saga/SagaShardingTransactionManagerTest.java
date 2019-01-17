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

package io.shardingsphere.transaction.saga;

import io.shardingsphere.transaction.saga.resource.SagaResourceManager;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.servicecomb.transport.ShardingSQLTransport;
import io.shardingsphere.transaction.saga.servicecomb.transport.ShardingTransportFactory;
import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.executor.ShardingExecuteDataMap;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class SagaShardingTransactionManagerTest {
    
    private final SagaShardingTransactionManager sagaShardingTransactionManager = new SagaShardingTransactionManager();
    
    @Mock
    private SagaResourceManager sagaResourceManager;
    
    @Mock
    private SagaExecutionComponent sagaExecutionComponent;
    
    @Mock
    private SagaPersistence sagaPersistence;
    
    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        Field transactionManagerField = SagaShardingTransactionManager.class.getDeclaredField("resourceManager");
        transactionManagerField.setAccessible(true);
        transactionManagerField.set(sagaShardingTransactionManager, sagaResourceManager);
    }
    
    @After
    public void tearDown() {
        getTransactionThreadLocal().remove();
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows
    private ThreadLocal<SagaTransaction> getTransactionThreadLocal() {
        Field transactionField = SagaShardingTransactionManager.class.getDeclaredField("TRANSACTION");
        transactionField.setAccessible(true);
        return (ThreadLocal<SagaTransaction>) transactionField.get(SagaShardingTransactionManager.class);
    }
    
    @Test
    public void assertGetTransactionType() {
        assertThat(sagaShardingTransactionManager.getTransactionType(), equalTo(TransactionType.BASE));
    }
    
    @Test
    public void assertInit() {
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        sagaShardingTransactionManager.init(DatabaseType.MySQL, dataSourceMap);
        verify(sagaResourceManager).registerDataSourceMap(dataSourceMap);
    }
    
    @Test
    public void assertClose() {
        sagaShardingTransactionManager.close();
        verify(sagaResourceManager).releaseDataSourceMap();
    }
    
    @Test
    public void assertGetConnection() throws SQLException {
        SagaTransaction sagaTransaction = mock(SagaTransaction.class);
        ConcurrentMap<String, Connection> connectionMap = new ConcurrentHashMap<>();
        Connection connection = mock(Connection.class);
        when(sagaTransaction.getConnectionMap()).thenReturn(connectionMap);
        when(sagaResourceManager.getConnection("ds")).thenReturn(connection);
        getTransactionThreadLocal().set(sagaTransaction);
        assertThat(sagaShardingTransactionManager.getConnection("ds"), is(connection));
        assertThat(sagaTransaction.getConnectionMap().size(), is(1));
        assertThat(sagaTransaction.getConnectionMap().get("ds"), is(connection));
    }
    
    @Test
    public void assertBegin() {
        sagaShardingTransactionManager.begin();
        assertNotNull(SagaShardingTransactionManager.getTransaction());
        assertTrue(ShardingExecuteDataMap.getDataMap().containsKey(SagaShardingTransactionManager.TRANSACTION_KEY));
        assertThat(ShardingExecuteDataMap.getDataMap().get(SagaShardingTransactionManager.TRANSACTION_KEY), instanceOf(SagaTransaction.class));
        assertThat(ShardingTransportFactory.getInstance().getTransport(), instanceOf(ShardingSQLTransport.class));
    }
    
    @Test
    public void assertCommitWithoutBegin() {
        sagaShardingTransactionManager.commit();
        verify(sagaExecutionComponent, never()).run(anyString());
        assertNull(SagaShardingTransactionManager.getTransaction());
        assertTrue(ShardingExecuteDataMap.getDataMap().isEmpty());
        assertNull(ShardingTransportFactory.getInstance().getTransport());
    }
    
    @Test
    public void assertCommitWithException() throws NoSuchFieldException, IllegalAccessException {
        when(sagaResourceManager.getSagaExecutionComponent()).thenReturn(sagaExecutionComponent);
        when(sagaResourceManager.getSagaPersistence()).thenReturn(sagaPersistence);
        sagaShardingTransactionManager.begin();
        Field containExceptionField = SagaTransaction.class.getDeclaredField("containException");
        containExceptionField.setAccessible(true);
        containExceptionField.set(ShardingExecuteDataMap.getDataMap().get(SagaShardingTransactionManager.TRANSACTION_KEY), true);
        sagaShardingTransactionManager.commit();
        verify(sagaExecutionComponent).run(anyString());
        assertNull(SagaShardingTransactionManager.getTransaction());
        assertTrue(ShardingExecuteDataMap.getDataMap().isEmpty());
        assertNull(ShardingTransportFactory.getInstance().getTransport());
    }
    
    @Test
    public void assertCommitWithoutException() {
        when(sagaResourceManager.getSagaPersistence()).thenReturn(sagaPersistence);
        sagaShardingTransactionManager.begin();
        sagaShardingTransactionManager.commit();
        verify(sagaExecutionComponent, never()).run(anyString());
        assertNull(SagaShardingTransactionManager.getTransaction());
        assertTrue(ShardingExecuteDataMap.getDataMap().isEmpty());
        assertNull(ShardingTransportFactory.getInstance().getTransport());
    }
    
    @Test
    public void assertRollbackWithoutBegin() {
        sagaShardingTransactionManager.rollback();
        assertNull(SagaShardingTransactionManager.getTransaction());
        assertTrue(ShardingExecuteDataMap.getDataMap().isEmpty());
        assertNull(ShardingTransportFactory.getInstance().getTransport());
    }
    
    @Test
    public void assertRollbackWithBegin() {
        when(sagaResourceManager.getSagaExecutionComponent()).thenReturn(sagaExecutionComponent);
        when(sagaResourceManager.getSagaPersistence()).thenReturn(sagaPersistence);
        sagaShardingTransactionManager.begin();
        sagaShardingTransactionManager.rollback();
        assertNull(SagaShardingTransactionManager.getTransaction());
        assertTrue(ShardingExecuteDataMap.getDataMap().isEmpty());
        assertNull(ShardingTransportFactory.getInstance().getTransport());
    }
    
    @Test
    public void assertIsInTransaction() {
        sagaShardingTransactionManager.begin();
        assertTrue(sagaShardingTransactionManager.isInTransaction());
    }
    
    @Test
    public void assertIsNotInTransaction() {
        assertFalse(sagaShardingTransactionManager.isInTransaction());
    }
}
