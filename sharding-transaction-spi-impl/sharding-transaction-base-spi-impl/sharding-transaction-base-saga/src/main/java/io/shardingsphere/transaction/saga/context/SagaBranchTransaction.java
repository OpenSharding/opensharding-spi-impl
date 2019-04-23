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

package io.shardingsphere.transaction.saga.context;

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.revert.RevertSQLResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * Saga branch transaction.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class SagaBranchTransaction {
    
    private final String dataSourceName;
    
    private final String sql;
    
    private final List<List<Object>> parameterSets;
    
    private ExecuteStatus executeStatus = ExecuteStatus.EXECUTING;
    
    private RevertSQLResult revertSQLResult;
    
    public SagaBranchTransaction(final String dataSourceName, final String sql, final List<List<Object>> parameterSets, final ExecuteStatus executeStatus) {
        this(dataSourceName, sql, parameterSets);
        this.executeStatus = executeStatus;
    }
    
    public boolean isExecuteSQL() {
        return ExecuteStatus.COMPENSATING.equals(executeStatus) || ExecuteStatus.FAILURE.equals(executeStatus);
    }
    
    @Override
    public String toString() {
        return "SagaBranchTransaction{" + "dataSourceName='" + dataSourceName + '\'' + ", sql='" + sql + '\''
            + ", parameterSets=" + parameterSets + ", executeStatus=" + executeStatus + ", revertSQLResult=" + revertSQLResult + '}';
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
