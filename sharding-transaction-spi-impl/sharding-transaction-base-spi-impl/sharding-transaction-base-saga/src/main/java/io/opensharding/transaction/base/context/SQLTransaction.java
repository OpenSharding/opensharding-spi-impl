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

package io.opensharding.transaction.base.context;

import io.opensharding.transaction.base.hook.revert.RevertSQLResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * SQL transaction.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class SQLTransaction {
    
    private String sqlTransactionId = UUID.randomUUID().toString().replaceAll("-", "");
    
    private final String dataSourceName;
    
    private final String sql;
    
    private final List<Collection<Object>> parameters;
    
    private ExecuteStatus executeStatus = ExecuteStatus.EXECUTING;
    
    private RevertSQLResult revertSQLResult;
    
    public SQLTransaction(final String dataSourceName, final String sql, final List<Collection<Object>> parameters, final ExecuteStatus executeStatus) {
        this(dataSourceName, sql, parameters);
        this.executeStatus = executeStatus;
    }
    
    @Override
    public String toString() {
        return "SagaBranchTransaction{" + "dataSourceName='" + dataSourceName + '\'' + ", sql='" + sql + '\''
            + ", parameters=" + parameters + ", executeStatus=" + executeStatus + ", revertSQLResult=" + revertSQLResult + '}';
    }
}
