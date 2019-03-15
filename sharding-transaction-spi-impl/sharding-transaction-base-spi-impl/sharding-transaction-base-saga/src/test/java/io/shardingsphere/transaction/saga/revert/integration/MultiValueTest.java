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

package io.shardingsphere.transaction.saga.revert.integration;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.api.RevertParameter;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.revert.impl.insert.RevertInsert;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.parse.SQLParsingEngine;
import org.apache.shardingsphere.core.parse.parser.sql.SQLStatement;
import org.apache.shardingsphere.core.parse.parser.sql.dml.DMLStatement;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiValueTest extends AbstractIntegrationTest {
    
    @Test
    public void assertMultiInsert() throws Exception {
        String insertSQL = "insert into t_order_history values(?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?)";
        List<Object> params = buildMultiInsertParam();
        Connection connection = dataSource.getConnection();
        update(connection, insertSQL, params);
        checkUpdate(connection, "1", 4);
        SQLStatement statement = new SQLParsingEngine(DatabaseType.MySQL, insertSQL, shardingRule, shardingTableMetaData, parsingResultCache).parse(true);
        String actualSQLTemplate = "insert into t_order_history_%S values(?,?,?,?)";
        for (int i = 0; i < 2; i++) {
            String actualSQL = String.format(actualSQLTemplate, i);
            Connection actualConnection = config.getDataSources().get("ds_" + i).getConnection();
            List<Object> reverParams = new LinkedList<>();
            for (int j = 0; j < params.size(); j++) {
                if (i == ((j / 4) + 1) % 2) {
                    reverParams.add(params.get(j));
                }
            }
            revertInsert((DMLStatement) statement, actualConnection, insertSQL, actualSQL, "t_order_history_" + i, reverParams);
            actualConnection.close();
        }
        checkUpdate(connection, "1", 0);
        connection.close();
    }
    
    private List<Object> buildMultiInsertParam() {
        List<Object> params = new LinkedList<>();
        for (int i = 1; i <= 4; i++) {
            params.add(i);
            params.add(i);
            params.add("1");
            params.add(new Date());
        }
        return params;
    }
    
    private void checkUpdate(final Connection connection, final String staus, final int expectCount) throws Exception {
        PreparedStatement queryStatement = connection.prepareStatement("select count(1) from t_order_history where status = ?");
        queryStatement.setObject(1, staus);
        ResultSet resultSet = queryStatement.executeQuery();
        resultSet.next();
        assertThat("Assert updated row count: ", resultSet.getInt(1), is(expectCount));
    }
    
    private void update(final Connection connection, final String insertSQL, final List<Object> params) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
        int index = 1;
        for (Object each : params) {
            preparedStatement.setObject(index++, each);
        }
        preparedStatement.executeUpdate();
    }
    
    private void revertInsert(final DMLStatement dmlStatement, final Connection actualConnection, final String insertSQL, final String actualSQL, final String actualTable,
                              final List<Object> params) throws Exception {
        SnapshotParameter snapshotParameter = new SnapshotParameter(shardingTableMetaData.get("t_order_history"), dmlStatement, actualConnection, actualTable, insertSQL, actualSQL, params);
        RevertInsert revertInsert = new RevertInsert();
        Optional<RevertContext> revertContext = revertInsert.snapshot(snapshotParameter);
        for (Collection<Object> each : revertContext.get().getRevertParams()) {
            RevertParameter revertParameter = new RevertParameter(actualConnection, revertContext.get().getRevertSQL(), each);
            revertInsert.revert(revertParameter);
        }
    }
    
}
