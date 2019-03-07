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

import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.yaml.config.sharding.YamlRootShardingConfiguration;
import org.apache.shardingsphere.core.yaml.engine.YamlEngine;
import org.apache.shardingsphere.core.yaml.swapper.impl.ShardingRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.ShardingDataSource;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractIntegrationTest {
    
    // CHECKSTYLE:OFF
    protected static DataSource dataSource;
    
    protected static YamlRootShardingConfiguration config;
    
    protected static ShardingRule shardingRule;
    
    protected static ShardingTableMetaData shardingTableMetaData;
    // CHECKSTYLE:ON
    
    private static final String DROP_HISTORY_TABLE = "DROP TABLE t_order_history;";
    
    private static final String CREATE_HISTORY_TABLE = "CREATE TABLE t_order_history (user_id int(11) NOT NULL,order_id int(11) NOT NULL, "
        + "status varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL, operate_date datetime(0) NULL DEFAULT NULL,PRIMARY KEY (user_id, order_id) USING BTREE);";
    
    private static final String DROP_ITEM_TABLE = "DROP TABLE IF EXISTS t_order_item";
    
    private static final String CREATE_ITEM_TABLE = "CREATE TABLE IF NOT EXISTS t_order_item(order_item_id bigint AUTO_INCREMENT PRIMARY KEY, "
        + "order_id bigint NOT NULL, user_id int(11) NOT NULL, status varchar(255));";
    
    @BeforeClass
    public static void initEnvironment() throws Exception {
        createShardingDatasource();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        initTables(statement, DROP_HISTORY_TABLE, CREATE_HISTORY_TABLE);
        initTables(statement, DROP_ITEM_TABLE, CREATE_ITEM_TABLE);
        statement.close();
        connection.close();
    }
    
    private static void initTables(final Statement statement, final String dropTableSQL, final String createTableSQL) throws SQLException {
        try {
            statement.execute(dropTableSQL);
            // CHECKSTYLE:OFF
        } catch (Exception e) {
            // CHECKSTYLE:ON
        }
        statement.execute(createTableSQL);
    }
    
    private static void createShardingDatasource() throws Exception {
        InputStream inputStream = MultiKeyTest.class.getClassLoader().getResourceAsStream("config-sharding.yaml");
        byte[] bytes = new byte[inputStream.available()];
        inputStream.read(bytes);
        config = YamlEngine.unmarshal(bytes, YamlRootShardingConfiguration.class);
        ShardingRuleConfiguration shardingRuleConfiguration = new ShardingRuleConfigurationYamlSwapper().swap(config.getShardingRule());
        dataSource = ShardingDataSourceFactory.createDataSource(config.getDataSources(), shardingRuleConfiguration, config.getProps());
        shardingRule = new ShardingRule(shardingRuleConfiguration, config.getDataSources().keySet());
        shardingTableMetaData = ((ShardingDataSource) dataSource).getShardingContext().getMetaData().getTable();
    }
    
}
