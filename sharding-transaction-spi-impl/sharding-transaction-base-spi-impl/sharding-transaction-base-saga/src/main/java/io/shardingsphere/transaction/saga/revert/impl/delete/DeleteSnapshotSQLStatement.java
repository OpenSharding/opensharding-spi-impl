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

import io.shardingsphere.transaction.saga.revert.api.SnapshotSQLStatement;
import lombok.Getter;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.dml.DeleteStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Delete snapshot SQL statement.
 *
 * @author zhaojun
 */
public final class DeleteSnapshotSQLStatement implements SnapshotSQLStatement {
    
    @Getter
    private final String actualTableName;
    
    private final DeleteStatement deleteStatement;
    
    private final List<Object> actualSQLParameters;
    
    public DeleteSnapshotSQLStatement(final String actualTableName, final DeleteStatement deleteStatement, final List<Object> actualSQLParameters) {
        this.actualTableName = actualTableName;
        this.deleteStatement = deleteStatement;
        this.actualSQLParameters = actualSQLParameters;
    }
    
    @Override
    public Collection<String> getQueryColumnNames() {
        return Collections.singleton("*");
    }
    
    @Override
    public String getTableAliasLiterals() {
        return "";
    }
    
    @Override
    public String getWhereClauseLiterals() {
        return 0 < deleteStatement.getWhereStartIndex() ? deleteStatement.getLogicSQL().substring(deleteStatement.getWhereStartIndex(), deleteStatement.getWhereStopIndex() + 1) : "";
    }
    
    @Override
    public Collection<Object> getQueryParameters() {
        Collection<Object> result = new LinkedList<>();
        for (int i = deleteStatement.getWhereParameterStartIndex(); i <= deleteStatement.getWhereParameterEndIndex(); i++) {
            result.add(actualSQLParameters.get(i));
        }
        return result;
    }
}
