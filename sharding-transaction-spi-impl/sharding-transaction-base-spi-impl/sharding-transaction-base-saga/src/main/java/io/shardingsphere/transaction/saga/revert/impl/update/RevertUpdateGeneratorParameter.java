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

package io.shardingsphere.transaction.saga.revert.impl.update;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.shardingsphere.transaction.saga.revert.impl.delete.RevertDeleteParameter;
import lombok.Getter;

/**
 * Revert update generator parameter.
 *
 * @author duhongjun
 */
@Getter
public final class RevertUpdateGeneratorParameter extends RevertDeleteParameter {
    
    private final List<String> updateColumns = new LinkedList<>();
    
    private final List<String> keys = new LinkedList<>();
    
    private final List<Object> params = new LinkedList<>();
    
    public RevertUpdateGeneratorParameter(final String tableName, final List<Map<String, Object>> selectSnapshot, final List<String> updateColumns, final List<String> keys,
                                          final List<Object> params) {
        super(tableName, selectSnapshot);
        this.updateColumns.addAll(updateColumns);
        this.keys.addAll(keys);
        this.params.addAll(params);
    }
}
