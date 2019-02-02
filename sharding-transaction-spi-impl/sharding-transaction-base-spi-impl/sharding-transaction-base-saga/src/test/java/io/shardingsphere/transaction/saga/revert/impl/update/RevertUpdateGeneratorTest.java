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

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.shardingsphere.transaction.saga.revert.impl.update.RevertUpdateGenerator;
import io.shardingsphere.transaction.saga.revert.impl.update.RevertUpdateGeneratorParameter;

public class RevertUpdateGeneratorTest extends BaseUpdateTest {
    
    @Test
    public void testGenerate() throws SQLException {
        List<Map<String, Object>> selectSnapshot = new LinkedList<>();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ORDER_ITEM_ID", ORDER_ITEM_ID);
        selectSnapshot.add(values);
        Map<String, Object> updateColumns = new LinkedHashMap<>();
        updateColumns.put("status", STATUS);
        updateColumns.put("order_id", ORDER_ID);
        updateColumns.put("user_id", USER_ID);
        values.putAll(updateColumns);
        List<Object> params = new LinkedList<>();
        params.add(STATUS);
        params.add(ORDER_ID);
        params.add(USER_ID);
        params.add(ORDER_ITEM_ID);
        List<String> keys = new LinkedList<>();
        keys.add("ORDER_ITEM_ID");
        RevertUpdateGeneratorParameter revertUpdateParameter = new RevertUpdateGeneratorParameter("t_order_item_1", selectSnapshot, updateColumns, keys, params);
        RevertUpdateGenerator revertUpdateGenerator = new RevertUpdateGenerator();
        asertRevertContext(revertUpdateGenerator.generate(revertUpdateParameter), REVERT_SQL);
    }
}
