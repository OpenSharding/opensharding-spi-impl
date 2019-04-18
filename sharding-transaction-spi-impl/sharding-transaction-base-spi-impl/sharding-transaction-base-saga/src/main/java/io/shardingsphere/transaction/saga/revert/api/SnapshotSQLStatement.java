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

package io.shardingsphere.transaction.saga.revert.api;

import java.util.Collection;

/**
 * Snapshot SQL segment.
 *
 * @author zhaojun
 */
public interface SnapshotSQLStatement {
    
    /**
     * Get actual table name.
     *
     * @return table name
     */
    String getActualTableName();
    
    /**
     * Get query items.
     *
     * @return query items
     */
    Collection<String> getQueryColumnNames();
    
    /**
     * Get Table alias literals.
     *
     * @return table alias
     */
    String getTableAliasLiterals();
    
    /**
     * Get where clause literals.
     *
     * @return where clause literals
     */
    String getWhereClauseLiterals();
    
    /**
     * Get Query parameters.
     *
     * @return Collection
     */
    Collection<Object> getQueryParameters();
}
