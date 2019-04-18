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

package io.shardingsphere.transaction.saga.revert.impl.delete;

import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;

import org.apache.shardingsphere.core.metadata.table.ColumnMetaData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class RevertDeleteTest {
    
//    @Mock
//    private DeleteRevertSQLGenerator revertDeleteGenerator;
//
//    @Mock
//    private DeleteSnapshotMaker snapshotMaker;
//
//    private SnapshotParameter snapshotParameter;
//
//    @Before
//    public void setUp() throws Exception {
//        snapshotParameter = new SnapshotParameter(TableMetaDataUtil.mockTableMetaData(), null, null, TableMetaDataUtil.ACTUAL_TABLE_NAME, null, null, null);
//    }
//
//    @Test
//    public void assertSnapshot() throws Exception {
//        DeleteRevertSQLExecuteWrapper revertDelete = new DeleteRevertSQLExecuteWrapper();
//        revertDelete.setSnapshotMaker(snapshotMaker);
//        revertDelete.setRevertSQLGenerator(revertDeleteGenerator);
//        revertDelete.snapshot(snapshotParameter);
//        verify(snapshotMaker).make(eq(snapshotParameter), ArgumentMatchers.<String>anyList());
//        verify(revertDeleteGenerator).generate(any(DeleteRevertSQLContext.class));
//    }
//
//    @Test(expected = RuntimeException.class)
//    public void assertSnapshotNoKey() throws Exception {
//        DeleteRevertSQLExecuteWrapper revertDelete = new DeleteRevertSQLExecuteWrapper();
//        revertDelete.setSnapshotMaker(snapshotMaker);
//        revertDelete.setRevertSQLGenerator(revertDeleteGenerator);
//        snapshotParameter.getTableMeta().getColumns().put(TableMetaDataUtil.COLUMN_ORDER_ID, new ColumnMetaData(TableMetaDataUtil.COLUMN_ORDER_ID, "", false));
//        revertDelete.snapshot(snapshotParameter);
//    }
}
