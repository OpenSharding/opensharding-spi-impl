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

package io.shardingsphere.transaction.saga.revert.impl;

import io.shardingsphere.transaction.saga.revert.impl.delete.DeleteRevertSQLExecutor;
import io.shardingsphere.transaction.saga.revert.impl.insert.RevertInsert;
import io.shardingsphere.transaction.saga.revert.impl.update.RevertUpdate;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class RevertOperateFactoryTest {
    
    @Test
    public void assertGetRevertSQLCreator() {
        RevertOperateFactory factory = new RevertOperateFactory();
        assertThat(factory.getRevertSQLCreator(mock(InsertStatement.class)), instanceOf(RevertInsert.class));
        assertThat(factory.getRevertSQLCreator(mock(UpdateStatement.class)), instanceOf(RevertUpdate.class));
        assertThat(factory.getRevertSQLCreator(mock(DeleteStatement.class)), instanceOf(DeleteRevertSQLExecutor.class));
    }
}
