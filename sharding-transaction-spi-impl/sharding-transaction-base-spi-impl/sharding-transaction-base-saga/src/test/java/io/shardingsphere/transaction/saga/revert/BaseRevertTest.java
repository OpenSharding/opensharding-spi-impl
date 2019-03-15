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

package io.shardingsphere.transaction.saga.revert;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.api.RevertContext;
import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.SQLParsingEngine;
import org.apache.shardingsphere.core.parse.cache.ParsingResultCache;
import org.apache.shardingsphere.core.parse.parser.sql.SQLStatement;
import org.apache.shardingsphere.core.parse.parser.sql.dml.DMLStatement;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.yaml.config.sharding.YamlRootShardingConfiguration;
import org.apache.shardingsphere.core.yaml.engine.YamlEngine;
import org.apache.shardingsphere.core.yaml.swapper.impl.ShardingRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.ShardingDataSource;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class BaseRevertTest {
    
    public static final long ORDER_ITEM_ID = 3;
    
    public static final int USER_ID = 3;
    
    public static final long ORDER_ID = 3;
    
    public static final String STATUS = "1";
    
    public static final int NEW_USER_ID = 4;
    
    public static final long NEW_ORDER_ID = 4;
    
    public static final String NEW_STATUS = "2";
    
    private static DataSource actualDataSource;
    
    private static ShardingRule shardingRule;
    
    private static ShardingTableMetaData shardingTableMetaData;
    
    private static ParsingResultCache parsingResultCache;
    
    static {
        try {
            InputStream inputStream = BaseRevertTest.class.getClassLoader().getResourceAsStream("config-sharding.yaml");
            byte[] bytes = new byte[inputStream.available()];
            inputStream.read(bytes);
            YamlRootShardingConfiguration config = YamlEngine.unmarshal(bytes, YamlRootShardingConfiguration.class);
            ShardingRuleConfiguration shardingRuleConfiguration = new ShardingRuleConfigurationYamlSwapper().swap(config.getShardingRule());
            ShardingDataSource shardingDataSource = (ShardingDataSource) ShardingDataSourceFactory.createDataSource(config.getDataSources(), shardingRuleConfiguration, config.getProps());
            shardingRule = new ShardingRule(shardingRuleConfiguration, config.getDataSources().keySet());
            shardingTableMetaData = ((ShardingDataSource) shardingDataSource).getShardingContext().getMetaData().getTable();
            actualDataSource = shardingDataSource.getDataSourceMap().get("ds_1");
            parsingResultCache = new ParsingResultCache();
            // CHECKSTYLE:OFF
        } catch (Exception e) {
            // CHECKSTYLE:ON
            throw new RuntimeException("Failed to init data source", e);
        }
    }
    
    @Before
    public void setup() throws Exception {
        List<Object> params = new LinkedList<>();
        params.add(ORDER_ITEM_ID);
        params.add(ORDER_ID);
        params.add(USER_ID);
        params.add(STATUS);
        Connection connection = getConnection();
        String insertSQL = "insert into t_order_item_1 values(?,?,?,?)";
        JDBCUtil.executeUpdate(connection, insertSQL, params);
        connection.close();
    }
    
    @After
    public void teardown() throws Exception {
        List<Object> params = new LinkedList<>();
        params.add(ORDER_ITEM_ID);
        String insertSQL = "delete from t_order_item_1 where order_item_id=?";
        Connection connection = getConnection();
        JDBCUtil.executeUpdate(connection, insertSQL, params);
        connection.close();
    }
    
    protected void asertRevertContext(final Optional<RevertContext> revertContext, final String revertSQL, final int expectedParamSize) throws SQLException {
        assertTrue("Assert revert context exists: ", revertContext.isPresent());
        assertThat("Assert revert sql: ", revertSQL.toLowerCase(), is(revertContext.get().getRevertSQL().toLowerCase()));
        assertThat("Assert revert execute times: ", revertContext.get().getRevertParams().size(), is(1));
        assertThat("Assert revert param size: ", revertContext.get().getRevertParams().get(0).size(), is(expectedParamSize));
    }
    
    protected SnapshotParameter createParameter(final Connection connection, final String logicTable,
                                                final String actualTable, final String logicSQL, final String actualSQL, final List<Object> params) {
        SQLStatement statement = new SQLParsingEngine(DatabaseType.MySQL, logicSQL, shardingRule, shardingTableMetaData, parsingResultCache).parse(true);
        assertTrue("SQL Revert must be DML statemeent", statement instanceof DMLStatement);
        return new SnapshotParameter(shardingTableMetaData.get(logicTable), (DMLStatement) statement, connection, actualTable,
                logicSQL, actualSQL, params);
    }
    
    protected static Connection getConnection() {
        try {
            return actualDataSource.getConnection();
            // CHECKSTYLE:OFF
        } catch (Exception e) {
            // CHECKSTYLE:ON
            throw new RuntimeException("Failed to get JDBC connection:" + e.getMessage());
        }
    }
}
