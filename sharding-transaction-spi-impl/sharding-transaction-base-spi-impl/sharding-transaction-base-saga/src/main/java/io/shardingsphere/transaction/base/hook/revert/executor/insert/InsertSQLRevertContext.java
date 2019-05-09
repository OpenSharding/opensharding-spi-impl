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

package io.shardingsphere.transaction.base.hook.revert.executor.insert;

import com.google.common.base.Preconditions;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertContext;
import lombok.Getter;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResult;
import org.apache.shardingsphere.core.optimize.result.insert.InsertOptimizeResultUnit;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLIgnoreExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLNumberExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLParameterMarkerExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;
import org.apache.shardingsphere.core.rule.DataNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Insert SQL revert context.
 *
 * @author zhaojun
 */
@Getter
public final class InsertSQLRevertContext implements SQLRevertContext {
    
    private final String actualTable;
    
    private String dataSourceName;
    
    private final Collection<Map<String, Object>> primaryKeyInsertValues = new LinkedList<>();

    public InsertSQLRevertContext(final String dataSourceName, final String actualTableName, final List<String> primaryKeys, final InsertOptimizeResult insertOptimizeResult) {
        this.dataSourceName = dataSourceName;
        this.actualTable = actualTableName;
        loadPrimaryKeyInsertValues(dataSourceName, actualTableName, primaryKeys, insertOptimizeResult);
    }
    
    private void loadPrimaryKeyInsertValues(final String dataSourceName, final String actualTableName, final List<String> primaryKeys, final InsertOptimizeResult insertOptimizeResult) {
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
        Iterator<Object> parametersIterator = Arrays.asList(insertOptimizeResultUnit.getParameters()).iterator();
        for (SQLExpression each : insertOptimizeResultUnit.getValues()) {
            if (each instanceof SQLParameterMarkerExpression) {
                result.put(columnNamesIterator.next(), parametersIterator.next());
            } else if (each instanceof SQLTextExpression) {
                result.put(columnNamesIterator.next(), ((SQLTextExpression) each).getText());
            } else if (each instanceof SQLNumberExpression) {
                result.put(columnNamesIterator.next(), ((SQLNumberExpression) each).getNumber());
            } else if (each instanceof SQLIgnoreExpression) {
                result.put(columnNamesIterator.next(), ((SQLIgnoreExpression) each).getExpression());
            }
        }
        return result;
    }
    
    private void addPrimaryKeyColumnValues(final Map<String, Object> routedInsertValue, final List<String> primaryKeys) {
        Map<String, Object> primaryKeyInsertValue = new LinkedHashMap<>(primaryKeys.size(), 1);
        for (String each : primaryKeys) {
            if (routedInsertValue.containsKey(each)) {
                primaryKeyInsertValue.put(each, routedInsertValue.get(each));
            }
        }
        if (!primaryKeyInsertValue.isEmpty()) {
            primaryKeyInsertValues.add(primaryKeyInsertValue);
        }
    }
}
