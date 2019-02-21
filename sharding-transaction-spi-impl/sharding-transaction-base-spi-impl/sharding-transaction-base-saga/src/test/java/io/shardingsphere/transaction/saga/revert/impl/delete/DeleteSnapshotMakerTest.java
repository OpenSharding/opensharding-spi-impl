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

import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import org.junit.Test;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class DeleteSnapshotMakerTest extends BaseDeleteTest {
    
    @Test
    public void assertMake() throws SQLException {
        List<Object> params = new LinkedList<>();
        params.add(ORDER_ITEM_ID);
        String logicSQL = "delete from t_order_item where order_item_id=?";
        String actualSQL = "delete t_order_item_1 t_order_item where order_item_id=?";
        SnapshotParameter snapshotParameter = this.createParameter(getConnection(), "t_order_item", "t_order_item_1", logicSQL, actualSQL, params);
        List<String> keys = new LinkedList<>();
        keys.add("order_item_id");
        DeleteSnapshotMaker maker = new DeleteSnapshotMaker();
        List<Map<String, Object>> snapshot = maker.make(snapshotParameter, keys);
        assertTrue("Snapshot size error: ", snapshot.size() == 1);
        Map<String, Object> row = snapshot.get(0);
        assertTrue("Assert ORDER_ITEM_ID value error: ", row.get("order_item_id").equals(ORDER_ITEM_ID));
        assertTrue("Assert ORDER_ID value error: ", row.get("order_id").equals(ORDER_ID));
        assertTrue("Assert USER_ID value error: ", row.get("user_id").equals(USER_ID));
        assertTrue("Assert STATUS value error: ", row.get("status").equals(STATUS));
        snapshotParameter.getConnection().close();
    }
}
