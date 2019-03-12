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

package io.shardingsphere.transaction.saga.revert.impl.delete;

import org.junit.Test;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RevertDeleteGeneratorTest extends BaseDeleteTest {
    
    @Test
    public void testGenerate() throws SQLException {
        List<Map<String, Object>> selectSnapshot = new LinkedList<>();
        Map<String, Object> values = new LinkedHashMap<>();
        selectSnapshot.add(values);
        values.put("ORDER_ITEM_ID", ORDER_ITEM_ID);
        values.put("ORDER_ID", ORDER_ID);
        values.put("USER_ID", USER_ID);
        values.put("STATUS", STATUS);
        RevertDeleteParameter revertDeleteParameter = new RevertDeleteParameter("t_order_item_1", selectSnapshot);
        RevertDeleteGenerator revertDeleteGenerator = new RevertDeleteGenerator();
        assertRevertContext(revertDeleteGenerator.generate(revertDeleteParameter), REVERT_SQL);
    }
}
