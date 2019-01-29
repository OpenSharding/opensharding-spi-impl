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

package io.shardingsphere.transaction.saga.resource;

import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import lombok.SneakyThrows;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class SagaResourceManagerTest {
    
    private final SagaResourceManager resourceManager = new SagaResourceManager(new SagaConfiguration());
    
    @Test
    public void assertRegisterDataSourceMap() {
        resourceManager.registerDataSourceMap("ds1", mock(DataSource.class));
        assertThat(getDataSourceMap().size(), is(1));
        resourceManager.registerDataSourceMap("ds2", mock(DataSource.class));
        resourceManager.registerDataSourceMap("ds3", mock(DataSource.class));
        assertThat(getDataSourceMap().size(), is(3));
    }
    
    @Test(expected = ShardingException.class)
    public void assertRegisterDuplicateDataSourceMap() {
        resourceManager.registerDataSourceMap("ds1", mock(DataSource.class));
        assertThat(getDataSourceMap().size(), is(1));
        resourceManager.registerDataSourceMap("ds2", mock(DataSource.class));
        resourceManager.registerDataSourceMap("ds1", mock(DataSource.class));
    }
    
    @Test
    public void assertReleaseDataSourceMap() {
        resourceManager.registerDataSourceMap("ds1", mock(DataSource.class));
        resourceManager.registerDataSourceMap("ds2", mock(DataSource.class));
        resourceManager.registerDataSourceMap("ds3", mock(DataSource.class));
        assertThat(getDataSourceMap().size(), is(3));
        resourceManager.releaseDataSourceMap();
        assertTrue(getDataSourceMap().isEmpty());
    }
    
    @Test
    @SneakyThrows
    public void assertGetConnection() {
        DataSource dataSource = mock(DataSource.class);
        resourceManager.registerDataSourceMap("ds1", dataSource);
        resourceManager.getConnection("ds1");
        verify(dataSource).getConnection();
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows
    private Map<String, DataSource> getDataSourceMap() {
        Field field = SagaResourceManager.class.getDeclaredField("dataSourceMap");
        field.setAccessible(true);
        return (Map) field.get(resourceManager);
    }
}
