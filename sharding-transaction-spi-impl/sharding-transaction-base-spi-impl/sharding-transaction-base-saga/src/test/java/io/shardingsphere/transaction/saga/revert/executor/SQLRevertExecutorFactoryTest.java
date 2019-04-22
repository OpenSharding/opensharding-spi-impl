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

package io.shardingsphere.transaction.saga.revert.executor;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.executor.delete.DeleteSQLRevertExecutor;
import io.shardingsphere.transaction.saga.revert.executor.insert.InsertSQLRevertExecutor;
import org.apache.shardingsphere.core.optimize.result.OptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SQLRevertExecutorFactoryTest {
    
    @Mock
    private SQLRevertExecutorContext executorContext;
    
    @Mock
    private InsertStatement insertStatement;
    
    @Mock
    private DeleteStatement deleteStatement;
    
    @Mock
    private OptimizeResult optimizeResult;
    
    @Mock
    private InsertOptimizeResult insertOptimizeResult;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    @Mock
    private ResultSetMetaData resultSetMetaData;
    
    private List<Object> parameters = new LinkedList<>();
    
    @Before
    public void setUp() {
    }
    
    @Test
    public void assertNewSQLRevertExecutor() {
        when(executorContext.getSqlStatement()).thenReturn(insertStatement);
        when(executorContext.getOptimizeResult()).thenReturn(optimizeResult);
        when(optimizeResult.getInsertOptimizeResult()).thenReturn(Optional.of(insertOptimizeResult));
        SQLRevertExecutor actual = SQLRevertExecutorFactory.newInstance(executorContext);
        assertThat(actual, instanceOf(InsertSQLRevertExecutor.class));
    }
    
    @Test
    public void assertDeleteSQLRevertExecutor() throws SQLException {
        when(executorContext.getSqlStatement()).thenReturn(deleteStatement);
        when(executorContext.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        SQLRevertExecutor actual = SQLRevertExecutorFactory.newInstance(executorContext);
        assertThat(actual, instanceOf(DeleteSQLRevertExecutor.class));
    }
}