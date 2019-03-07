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

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.api.RevertParameter;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RevertDeleteTest extends BaseDeleteTest {
    
    @Test
    public void asertSnapshot() throws SQLException {
        Connection connection = getConnection();
        assertRevertContext(createSnapshot(connection), REVERT_SQL);
        connection.close();
    }
    
    private Optional<RevertContext> createSnapshot(final Connection connection) throws SQLException {
        List<Object> params = new LinkedList<>();
        params.add(ORDER_ITEM_ID);
        String logicSQL = "delete from t_order_item where order_item_id=?";
        String actualSQL = "delete t_order_item_1 t_order_item where order_item_id=?";
        SnapshotParameter snapshotParameter = this.createParameter(connection, "t_order_item", "t_order_item_1", logicSQL, actualSQL, params);
        RevertDelete revertDelete = new RevertDelete();
        return revertDelete.snapshot(snapshotParameter);
    }
    
    @Test
    public void asertRevert() throws Exception {
        Connection connection = getConnection();
        Optional<RevertContext> revertContext = createSnapshot(connection);
        this.teardown();
        RevertParameter revertParameter = new RevertParameter(connection, revertContext.get().getRevertSQL(), revertContext.get().getRevertParams().get(0));
        List<String> keys = new LinkedList<>();
        keys.add("order_item_id");
        RevertDelete revertDelete = new RevertDelete();
        revertDelete.revert(revertParameter);
        PreparedStatement preparedStatement = connection.prepareStatement("select * from t_order_item_1 where order_item_id=?");
        preparedStatement.setObject(1, ORDER_ITEM_ID);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertTrue("Assert next result set: ", resultSet.next());
        assertThat("Assert ORDER_ITEM_ID value error: ", resultSet.getLong("ORDER_ITEM_ID"), is(ORDER_ITEM_ID));
        assertThat("Assert ORDER_ID value error: ", resultSet.getLong("ORDER_ID"), is(ORDER_ID));
        assertThat("Assert USER_ID value error: ", resultSet.getInt("USER_ID"), is(USER_ID));
        assertThat("Assert STATUS value error: ", resultSet.getString("STATUS"), is(STATUS));
        assertTrue("Assert result set has no next: ", !resultSet.next());
        connection.close();
    }
}
