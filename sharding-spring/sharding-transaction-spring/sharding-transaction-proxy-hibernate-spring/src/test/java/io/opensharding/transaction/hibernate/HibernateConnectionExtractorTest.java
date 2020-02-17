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

package io.opensharding.transaction.hibernate;

import io.opensharding.transaction.spi.JpaConnectionExtractor;
import org.apache.shardingsphere.core.spi.NewInstanceServiceLoader;
import org.hibernate.engine.spi.SessionImplementor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.persistence.EntityManager;
import java.sql.Connection;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HibernateConnectionExtractorTest {
    
    {
        NewInstanceServiceLoader.register(JpaConnectionExtractor.class);
    }
    
    @Mock
    private EntityManager entityManager;
    
    @Mock
    private SessionImplementor sessionImplementor;
    
    @Mock
    private Connection connection;
    
    @Before
    public void setUp() {
        when(entityManager.unwrap(SessionImplementor.class)).thenReturn(sessionImplementor);
        when(sessionImplementor.connection()).thenReturn(connection);
    }
    
    @Test
    public void assertGetConnectionFromEntityManager() {
        Collection<JpaConnectionExtractor> extractors = NewInstanceServiceLoader.newServiceInstances(JpaConnectionExtractor.class);
        assertThat(extractors.size(), is(1));
        assertThat(extractors.iterator().next().getConnectionFromEntityManager(entityManager), is(connection));
    }
}
