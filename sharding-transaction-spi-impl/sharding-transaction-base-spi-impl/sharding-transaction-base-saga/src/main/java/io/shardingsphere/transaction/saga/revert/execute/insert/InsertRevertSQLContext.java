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

package io.shardingsphere.transaction.saga.revert.execute.insert;

import com.google.common.base.Preconditions;
import io.shardingsphere.transaction.saga.revert.execute.RevertSQLContext;
import lombok.Getter;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResultUnit;
import org.apache.shardingsphere.core.rule.DataNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Insert revert SQL context.
 *
 * @author zhaojun
 */
@Getter
public final class InsertRevertSQLContext implements RevertSQLContext {
    
    private final String actualTable;
    
    private final Map<String, List<Object>> primaryKeyValues = new LinkedHashMap<>();

    public InsertRevertSQLContext(final String dataSourceName, final String actualTableName, final List<String> primaryKeys, final InsertOptimizeResult insertOptimizeResult) {
        this.actualTable = actualTableName;
        loadPrimaryKeyValues(dataSourceName, actualTableName, primaryKeys, insertOptimizeResult);
    }
    
    private void loadPrimaryKeyValues(final String dataSourceName, final String actualTableName, final List<String> primaryKeys, final InsertOptimizeResult insertOptimizeResult) {
        Preconditions.checkNotNull(insertOptimizeResult, "Could not found insert optimize result. datasourceName:%s, actualTable:%s", dataSourceName, actualTableName);
        for (Map<String, Object> each : getRoutedInsertValues(insertOptimizeResult.getUnits(), new DataNode(dataSourceName, actualTableName))) {
            addPrimaryKeyColumnValues(each, primaryKeys);
        }
    }
    
    private List<Map<String, Object>> getRoutedInsertValues(final List<InsertOptimizeResultUnit> units, final DataNode dataNode) {
        List<Map<String, Object>> result = new LinkedList<>();
        for (InsertOptimizeResultUnit each : units) {
            if (isRoutedDataNode(each.getDataNodes(), dataNode)) {
                result.add(convertInsertOptimizeResultUnit(each));
            }
        }
        return result;
    }
    
    private boolean isRoutedDataNode(final List<DataNode> dataNodes, final DataNode dataNode) {
        for (DataNode each : dataNodes) {
            if (each.equals(dataNode)) {
                return true;
            }
        }
        return false;
    }
    
    private Map<String, Object> convertInsertOptimizeResultUnit(final InsertOptimizeResultUnit insertOptimizeResultUnit) {
        Map<String, Object> result = new HashMap<>(insertOptimizeResultUnit.getColumnNames().size(), 1);
        Iterator<String> columnNamesIterator = insertOptimizeResultUnit.getColumnNames().iterator();
        for (Object each : insertOptimizeResultUnit.getParameters()) {
            result.put(columnNamesIterator.next(), each);
        }
        return result;
    }
    
    private void addPrimaryKeyColumnValues(final Map<String, Object> routedInsertValue, final List<String> primaryKeys) {
        for (Map.Entry<String, Object> entry : routedInsertValue.entrySet()) {
            if (primaryKeys.contains(entry.getKey())) {
                if (!primaryKeyValues.containsKey(entry.getKey())) {
                    primaryKeyValues.put(entry.getKey(), new LinkedList<>());
                }
                primaryKeyValues.get(entry.getKey()).add(entry.getValue());
            }
        }
    }
}
