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

package io.shardingsphere.transaction.saga.revert.impl.insert;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.shardingsphere.transaction.saga.revert.impl.insert.RevertInsertGenerator;
import io.shardingsphere.transaction.saga.revert.impl.insert.RevertInsertGeneratorParameter;

public class RevertInsertGeneratorTest extends BaseInsertTest {
    
    @Test
    public void testGenerate() throws SQLException {
        List<String> tableColumns = new LinkedList<>();
        tableColumns.add("STATUS");
        tableColumns.add("ORDER_ID");
        tableColumns.add("USER_ID");
        tableColumns.add("ORDER_ITEM_ID");
        List<String> keys = new LinkedList<>();
        keys.add("ORDER_ITEM_ID");
        List<Object> params = new LinkedList<>();
        params.add(ORDER_ITEM_ID);
        RevertInsertGeneratorParameter revertInsertParameter = new RevertInsertGeneratorParameter("t_order_item_1", tableColumns, keys, params, 1, false);
        Map<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("ORDER_ITEM_ID", ORDER_ITEM_ID);
        revertInsertParameter.getKeyValues().add(keyValues);
        RevertInsertGenerator revertInsertGenerator = new RevertInsertGenerator();
        asertRevertContext(revertInsertGenerator.generate(revertInsertParameter), REVERT_SQL);
    }
}
