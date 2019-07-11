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

package io.shardingsphere.transaction.base.context;

import lombok.Getter;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.parse.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.route.SQLRouteResult;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Logic SQL transaction.
 *
 * @author yangyi
 * @author zhaojun
 */
@Getter
public class LogicSQLTransaction {
    
    private final SQLRouteResult sqlRouteResult;
    
    private String logicTableName;
    
    private SQLStatement sqlStatement;
    
    private TableMetaData tableMetaData;
    
    private final Queue<BranchTransaction> branchTransactions = new ConcurrentLinkedQueue<>();
    
    public LogicSQLTransaction(final SQLRouteResult sqlRouteResult, final ShardingTableMetaData shardingTableMetaData) {
        this.sqlRouteResult = sqlRouteResult;
        logicTableName = sqlRouteResult.getOptimizedStatement().getSQLStatement().getTables().getSingleTableName();
        sqlStatement = sqlRouteResult.getOptimizedStatement().getSQLStatement();
        tableMetaData = shardingTableMetaData.get(logicTableName);
    }
    
    /**
     * Whether logic SQL is DML statement or not.
     *
     * @return true or false
     */
    public boolean isDMLLogicSQL() {
        return sqlStatement instanceof DMLStatement;
    }
}
