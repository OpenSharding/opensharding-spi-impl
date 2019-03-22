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

package io.shardingsphere.transaction.saga.revert;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaBranchTransactionGroup;
import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.impl.RevertOperateFactory;
import io.shardingsphere.transaction.saga.revert.impl.insert.RevertInsert;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.parser.context.table.Tables;
import org.apache.shardingsphere.core.parse.parser.sql.dml.insert.InsertStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import lombok.SneakyThrows;

@RunWith(MockitoJUnitRunner.class)
public class SQLRevertEngineTest {
    
    private final SQLRevertEngine revertEngine = new SQLRevertEngine(new HashMap<String, Connection>());
    
    @Mock
    private RevertOperateFactory factory;
    
    @Mock
    private InsertStatement dmlStatement;
    
    @Mock
    private RevertInsert revertInsert;
    
    @Before
    @SneakyThrows
    public void setUp() {
        Field revertOperateFactoryField = SQLRevertEngine.class.getDeclaredField("revertOperateFactory");
        revertOperateFactoryField.setAccessible(true);
        revertOperateFactoryField.set(revertEngine, factory);
        when(factory.getRevertSQLCreator(dmlStatement)).thenReturn(revertInsert);
        RevertContext revertContext = new RevertContext("revert SQL");
        revertContext.getRevertParams().add(Lists.<Object>newArrayList(1L));
        when(revertInsert.snapshot(any(SnapshotParameter.class))).thenReturn(Optional.of(revertContext));
    }
    
    @Test
    public void assertRevert() throws SQLException {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction("", "", Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList(TableMetaDataUtil.ORDER_ID_VALUE)));
        when(dmlStatement.getTables()).thenReturn(mock(Tables.class));
        SagaBranchTransactionGroup sagaBranchTransactionGroup = new SagaBranchTransactionGroup("", dmlStatement, mock(ShardingTableMetaData.class));
        SQLRevertResult result = revertEngine.revert(sagaBranchTransaction, sagaBranchTransactionGroup);
        assertThat(result.getSql(), is("revert SQL"));
        assertThat(result.getParameterSets().size(), is(1));
        assertThat((long) result.getParameterSets().get(0).iterator().next(), is(1L));
    }
}
