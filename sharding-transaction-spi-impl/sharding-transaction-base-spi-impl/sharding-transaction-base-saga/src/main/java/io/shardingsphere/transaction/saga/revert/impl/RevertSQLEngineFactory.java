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
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.UpdateStatement;

import java.sql.Connection;
import java.util.List;

/**
 * Revert SQL engine factory.
 *
 * @author duhongjun
 * @author zhaojun
 */
public final class RevertSQLEngineFactory {
    
    /**
     * Create new instance.
     *
     * @param actualTableName actual table name
     * @param sqlStatement  SQL statement
     * @param actualSQLParameters actual SQL parameters
     * @param tableMetaData table meta data
     * @param connection connection
     *
     * @return Revert Operate
     */
    public static RevertSQLEngine newInstance(final String actualTableName, final SQLStatement sqlStatement, final List<Object> actualSQLParameters,
                                              final TableMetaData tableMetaData, final Connection connection) {
        if (sqlStatement instanceof InsertStatement) {
            return new DMLRevertSQLEngine(new InsertRevertSQLExecuteWrapper(actualTableName, (InsertStatement) sqlStatement, actualSQLParameters));
        } else if (sqlStatement instanceof DeleteStatement) {
            return new DMLRevertSQLEngine(new DeleteRevertSQLExecuteWrapper(actualTableName, (DeleteStatement) sqlStatement, actualSQLParameters, connection));
        } else if (sqlStatement instanceof UpdateStatement) {
            return new DMLRevertSQLEngine(new UpdateRevertSQLExecuteWrapper(actualTableName, (UpdateStatement) sqlStatement, actualSQLParameters, tableMetaData, connection));
        } else {
            throw new UnsupportedOperationException("unsupported SQL statement");
        }
    }
}
