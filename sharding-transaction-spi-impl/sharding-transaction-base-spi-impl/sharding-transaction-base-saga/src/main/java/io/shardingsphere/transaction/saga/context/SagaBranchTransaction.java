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
    
    private ExecuteStatus executeStatus;
    
    private RevertSQLResult revertSQLUnit;
    
    public SagaBranchTransaction(final String dataSourceName, final String sql, final List<List<Object>> parameterSets, final ExecuteStatus executeStatus) {
        this(dataSourceName, sql, parameterSets);
        this.executeStatus = executeStatus;
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SagaBranchTransaction that = (SagaBranchTransaction) o;
        return dataSourceName.equals(that.getDataSourceName()) && sql.equals(that.sql) && parameterSets.equals(that.parameterSets);
    }
    
    //    @Override
//    public boolean equals(final Object obj) {
//        return this == obj || obj instanceof SagaBranchTransaction && this.toString().equals(obj.toString());
//    }
}
