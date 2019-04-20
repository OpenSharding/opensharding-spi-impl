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

package io.shardingsphere.transaction.saga.revert.executor.update;

import io.shardingsphere.transaction.saga.revert.executor.SQLRevertContext;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Update SQL revert context.
 *
 * @author duhongjun
 * @author zhaojun
 */
@Getter
public final class UpdateSQLRevertContext implements SQLRevertContext {
    
    private final String actualTable;
    
    private final List<Map<String, Object>> undoData = new LinkedList<>();
    
    private final Map<String, Object> updateSetAssignments = new LinkedHashMap<>();
    
    private final List<String> primaryKeyColumns = new LinkedList<>();
    
    private final List<Object> parameters = new LinkedList<>();
    
    public UpdateSQLRevertContext(final String tableName, final List<Map<String, Object>> undoData, final Map<String, Object> updateSetAssignments, final List<String> primaryKeyColumns,
                                  final List<Object> parameters) {
        this.actualTable = tableName;
        this.undoData.addAll(undoData);
        this.updateSetAssignments.putAll(updateSetAssignments);
        this.primaryKeyColumns.addAll(primaryKeyColumns);
        this.parameters.addAll(parameters);
    }
}
