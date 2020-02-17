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

package io.opensharding.transaction;

import io.opensharding.transaction.aspect.ShardingTransactionProxyAspect;
import io.opensharding.transaction.fixture.ShardingTransactionalTestService;
import io.opensharding.transaction.spi.JpaConnectionExtractor;

import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.spi.NewInstanceServiceLoader;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ContextConfiguration(locations = "classpath:shardingTransactionTest.xml")
public class ShardingTransactionalNameSpaceTest extends AbstractJUnit4SpringContextTests {
    
    {
        NewInstanceServiceLoader.register(JpaConnectionExtractor.class);
    }
    
    @Autowired
    private ShardingTransactionalTestService testService;
    
    @Autowired
    private ShardingTransactionProxyAspect aspect;
    
    private final Statement statement = mock(Statement.class);
    
    private final JpaTransactionManager jpaTransactionManager = mock(JpaTransactionManager.class);
    
    private final DataSourceTransactionManager dataSourceTransactionManager = mock(DataSourceTransactionManager.class);
    
    @Before
    public void setUp() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        EntityManager entityManager = mock(EntityManager.class);
        Connection connection = NewInstanceServiceLoader.newServiceInstances(JpaConnectionExtractor.class).iterator().next().getConnectionFromEntityManager(entityManager);
        EntityManagerFactory entityManagerFactory = mock(EntityManagerFactory.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(entityManagerFactory.createEntityManager()).thenReturn(entityManager);
        when(jpaTransactionManager.getEntityManagerFactory()).thenReturn(entityManagerFactory);
        when(dataSourceTransactionManager.getDataSource()).thenReturn(dataSource);
    }
    
    @Test(expected = ShardingException.class)
    public void assertChangeTransactionTypeForProxyWithIllegalTransactionManager() throws SQLException {
        aspect.setTransactionManager(mock(PlatformTransactionManager.class));
        testService.testChangeTransactionTypeToLOCAL();
    }
    
    @Test(expected = ShardingException.class)
    public void assertChangeTransactionTypeForProxyFailed() throws SQLException {
        when(statement.execute(anyString())).thenThrow(new SQLException("test switch exception"));
        aspect.setTransactionManager(dataSourceTransactionManager);
        testService.testChangeTransactionTypeToLOCAL();
    }
    
    @Test
    public void assertChangeTransactionTypeToLOCALForProxy() throws SQLException {
        when(statement.execute(anyString())).thenReturn(true);
        aspect.setTransactionManager(dataSourceTransactionManager);
        testService.testChangeTransactionTypeToLOCAL();
        aspect.setTransactionManager(jpaTransactionManager);
        testService.testChangeTransactionTypeToLOCAL();
        verify(statement, times(2)).execute("SCTL:SET TRANSACTION_TYPE=LOCAL");
    }
    
    @Test
    public void assertChangeTransactionTypeToXAForProxy() throws SQLException {
        when(statement.execute(anyString())).thenReturn(true);
        aspect.setTransactionManager(dataSourceTransactionManager);
        testService.testChangeTransactionTypeToXA();
        aspect.setTransactionManager(jpaTransactionManager);
        testService.testChangeTransactionTypeToXA();
        verify(statement, times(2)).execute("SCTL:SET TRANSACTION_TYPE=XA");
    }
    
    @Test
    public void assertChangeTransactionTypeToBASEForProxy() throws SQLException {
        when(statement.execute(anyString())).thenReturn(true);
        aspect.setTransactionManager(dataSourceTransactionManager);
        testService.testChangeTransactionTypeToBASE();
        aspect.setTransactionManager(jpaTransactionManager);
        testService.testChangeTransactionTypeToBASE();
        verify(statement, times(2)).execute("SCTL:SET TRANSACTION_TYPE=BASE");
    }
}
