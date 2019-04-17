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

import io.shardingsphere.transaction.saga.revert.api.RevertSQLEngine;
import io.shardingsphere.transaction.saga.revert.impl.delete.DeleteRevertSQLExecuteWrapper;
import io.shardingsphere.transaction.saga.revert.impl.insert.InsertRevertSQLExecuteWrapper;
import io.shardingsphere.transaction.saga.revert.impl.update.UpdateRevertSQLExecuteWrapper;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;

import java.sql.Connection;
import java.util.List;

/**
 * Revert operate factory.
 *
 * @author duhongjun
 */
public final class RevertOperateFactory {
    
    /**
     * Get RevertOperate by DMLStatement.
     *
     * @param actualTableName actual table name
     * @param dmlStatement  DML statement
     * @param actualSQLParameters actual SQL parameters
     * @param tableMetaData table meta data
     * @param connection connection
     *
     * @return Revert Operate
     */
    public RevertSQLEngine getRevertSQLCreator(final String actualTableName, final DMLStatement dmlStatement, final List<Object> actualSQLParameters, final TableMetaData tableMetaData,
                                               final Connection connection) {
        if (dmlStatement instanceof InsertStatement) {
            return new DMLRevertSQLEngine(new InsertRevertSQLExecuteWrapper(actualTableName, (InsertStatement) dmlStatement, actualSQLParameters));
        }
        if (dmlStatement instanceof DeleteStatement) {
            return new DMLRevertSQLEngine(new DeleteRevertSQLExecuteWrapper(actualTableName, (DeleteStatement) dmlStatement, actualSQLParameters, connection));
        }
        return new DMLRevertSQLEngine(new UpdateRevertSQLExecuteWrapper(actualTableName, (UpdateStatement) dmlStatement, actualSQLParameters, tableMetaData, connection));
    }
}
