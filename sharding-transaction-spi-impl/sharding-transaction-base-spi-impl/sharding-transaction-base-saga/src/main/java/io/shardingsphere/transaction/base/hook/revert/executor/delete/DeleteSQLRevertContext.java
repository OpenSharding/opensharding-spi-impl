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

package io.shardingsphere.transaction.base.hook.revert.executor.delete;

import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertContext;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Delete SQL revert context.
 *
 * @author duhongjun
 * @author zhaojun
 */
@Getter
public class DeleteSQLRevertContext implements SQLRevertContext {
    
    private final String actualTable;
    
    private final List<Map<String, Object>> undoData = new LinkedList<>();
    
    public DeleteSQLRevertContext(final String tableName, final List<Map<String, Object>> undoData) {
        this.actualTable = tableName;
        this.undoData.addAll(undoData);
    }
}
