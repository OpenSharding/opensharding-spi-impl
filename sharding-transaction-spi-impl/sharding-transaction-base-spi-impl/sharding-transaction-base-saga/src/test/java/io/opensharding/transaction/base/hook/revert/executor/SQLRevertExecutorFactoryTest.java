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

package io.opensharding.transaction.base.hook.revert.executor;

import com.google.common.base.Optional;
import io.opensharding.transaction.base.hook.revert.executor.delete.DeleteSQLRevertExecutor;
import io.opensharding.transaction.base.hook.revert.executor.insert.InsertSQLRevertExecutor;
import io.opensharding.transaction.base.hook.revert.executor.update.UpdateSQLRevertExecutor;
import org.apache.shardingsphere.core.optimize.sharding.statement.ShardingOptimizedStatement;
import org.apache.shardingsphere.core.optimize.sharding.statement.dml.ShardingInsertOptimizedStatement;
import org.apache.shardingsphere.core.parse.sql.segment.dml.assignment.SetAssignmentsSegment;
import org.apache.shardingsphere.core.parse.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.UpdateStatement;
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
    private UpdateStatement updateStatement;
    
    @Mock
    private SetAssignmentsSegment setAssignmentsSegment;
    
    @Mock
    private ShardingOptimizedStatement shardingStatement;
    
    @Mock
    private ShardingInsertOptimizedStatement shardingInsertOptimizedStatement;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    @Mock
    private ResultSetMetaData resultSetMetaData;
    
    private List<String> primaryKeyColumns = new LinkedList<>();
    
    @Before
    public void setUp() {
        when(executorContext.getShardingStatement()).thenReturn(shardingStatement);
        primaryKeyColumns.add("order_id");
        when(updateStatement.getSetAssignment()).thenReturn(setAssignmentsSegment);
        when(updateStatement.getWhere()).thenReturn(Optional.<WhereSegment>absent());
        when(deleteStatement.getWhere()).thenReturn(Optional.<WhereSegment>absent());
    }
    
    @Test
    public void assertNewSQLRevertExecutor() {
        when(executorContext.getShardingStatement()).thenReturn(shardingInsertOptimizedStatement);
        when(shardingInsertOptimizedStatement.getSQLStatement()).thenReturn(insertStatement);
        SQLRevertExecutor actual = SQLRevertExecutorFactory.newInstance(executorContext);
        assertThat(actual, instanceOf(InsertSQLRevertExecutor.class));
    }
    
    @Test
    public void assertNewDeleteSQLRevertExecutor() throws SQLException {
        when(shardingStatement.getSQLStatement()).thenReturn(deleteStatement);
        when(executorContext.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        SQLRevertExecutor actual = SQLRevertExecutorFactory.newInstance(executorContext);
        assertThat(actual, instanceOf(DeleteSQLRevertExecutor.class));
    }
    
    @Test
    public void assertNewUpdateSQLRevertExecutor() throws SQLException {
        when(shardingStatement.getSQLStatement()).thenReturn(updateStatement);
        when(executorContext.getConnection()).thenReturn(connection);
        when(executorContext.getPrimaryKeyColumns()).thenReturn(primaryKeyColumns);
        when(executorContext.getParameters()).thenReturn(Arrays.<Object>asList(1, 2, 3));
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        SQLRevertExecutor actual = SQLRevertExecutorFactory.newInstance(executorContext);
        assertThat(actual, instanceOf(UpdateSQLRevertExecutor.class));
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void assertNewUnsupportedSQLStatement() {
        when(shardingStatement.getSQLStatement()).thenReturn(mock(DMLStatement.class));
        SQLRevertExecutorFactory.newInstance(executorContext);
    }
}