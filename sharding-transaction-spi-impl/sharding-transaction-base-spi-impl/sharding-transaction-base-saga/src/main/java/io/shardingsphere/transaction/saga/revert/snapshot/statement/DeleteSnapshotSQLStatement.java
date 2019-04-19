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

package io.shardingsphere.transaction.saga.revert.snapshot.statement;

import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Delete snapshot SQL statement.
 *
 * @author zhaojun
 */
public final class DeleteSnapshotSQLStatement extends SnapshotSQLStatement {
    
    private final DeleteStatement deleteStatement;
    
    public DeleteSnapshotSQLStatement(final String actualTableName, final DeleteStatement deleteStatement, final List<Object> actualSQLParameters) {
        super(actualTableName, actualSQLParameters);
        this.deleteStatement = deleteStatement;
    }
    
    @Override
    public Collection<String> getQueryColumnNames() {
        return Collections.singleton("*");
    }
    
    @Override
    public String getWhereClause() {
        return 0 < deleteStatement.getWhereStartIndex() ? deleteStatement.getLogicSQL().substring(deleteStatement.getWhereStartIndex(), deleteStatement.getWhereStopIndex() + 1) : "";
    }
    
//    @Override
//    public Collection<Object> getParameters() {
//        Collection<Object> result = new LinkedList<>();
//        for (int i = deleteStatement.getWhereParameterStartIndex(); i <= deleteStatement.getWhereParameterEndIndex(); i++) {
//            result.add(actualSQLParameters.get(i));
//        }
//        return result;
//    }
}
