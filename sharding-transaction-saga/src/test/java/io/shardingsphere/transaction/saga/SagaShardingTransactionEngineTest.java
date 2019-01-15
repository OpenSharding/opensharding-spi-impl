/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.saga;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.transaction.api.TransactionType;
import io.shardingsphere.transaction.saga.manager.SagaResourceManager;
import io.shardingsphere.transaction.saga.manager.SagaTransactionManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.sql.DataSource;
import javax.transaction.Status;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class SagaShardingTransactionEngineTest {
    
    private final SagaShardingTransactionEngine sagaShardingTransactionEngine = new SagaShardingTransactionEngine();
    
    @Mock
    private SagaTransactionManager sagaTransactionManager;
    
    @Mock
    private SagaResourceManager sagaResourceManager;
    
    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        Field transactionManagerField = SagaShardingTransactionEngine.class.getDeclaredField("sagaTransactionManager");
        transactionManagerField.setAccessible(true);
        transactionManagerField.set(sagaShardingTransactionEngine, sagaTransactionManager);
        when(sagaTransactionManager.getResourceManager()).thenReturn(sagaResourceManager);
    }
    
    @Test
    public void assertGetTransactionType() {
        assertThat(sagaShardingTransactionEngine.getTransactionType(), equalTo(TransactionType.BASE));
    }
    
    @Test
    public void assertInit() {
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        sagaShardingTransactionEngine.init(DatabaseType.MySQL, dataSourceMap);
        verify(sagaResourceManager).registerDataSourceMap(dataSourceMap);
    }
    
    @Test
    public void assertClose() {
        sagaShardingTransactionEngine.close();
        verify(sagaResourceManager).releaseDataSourceMap();
    }
    
    @Test
    public void assertGetConnection() throws SQLException {
        SagaTransaction sagaTransaction = mock(SagaTransaction.class);
        Map<String, Connection> connectionMap = new HashMap<>();
        Connection connection = mock(Connection.class);
        when(sagaTransaction.getConnectionMap()).thenReturn(connectionMap);
        when(sagaResourceManager.getConnection("ds")).thenReturn(connection);
        when(sagaTransactionManager.getTransaction()).thenReturn(sagaTransaction);
        assertThat(sagaShardingTransactionEngine.getConnection("ds"), is(connection));
        assertThat(sagaTransaction.getConnectionMap().size(), is(1));
        assertThat(sagaTransaction.getConnectionMap().get("ds"), is(connection));
    }
    
    @Test
    public void assertBegin() {
        sagaShardingTransactionEngine.begin();
        verify(sagaTransactionManager).begin();
    }
    
    @Test
    public void assertCommit() {
        sagaShardingTransactionEngine.commit();
        verify(sagaTransactionManager).commit();
    }
    
    @Test
    public void assertRollback() {
        sagaShardingTransactionEngine.rollback();
        verify(sagaTransactionManager).rollback();
    }
    
    @Test
    public void assertIsInTransaction() {
        when(sagaTransactionManager.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        assertTrue(sagaShardingTransactionEngine.isInTransaction());
        when(sagaTransactionManager.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
        assertFalse(sagaShardingTransactionEngine.isInTransaction());
    }
}
