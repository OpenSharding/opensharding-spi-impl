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

package io.shardingsphere.transaction.saga.core.actuator.definition;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class SagaDefinitionBuilderTest {
    
    private static final List<List<Object>> INSERT_PARAMETERS = new ArrayList<>();
    
    private static final List<Collection<Object>> DELETE_PARAMETERS = new ArrayList<>();
    
    private static final List<List<Object>> UPDATE_PARAMETERS = new ArrayList<>();
    
    private static final List<Collection<Object>> UPDATE_C_PARAMETERS = new ArrayList<>();
    
    private static final String EXAMPLE_INSERT_SQL = "INSERT INTO TABLE ds_0.tb_0 (id, value) VALUES (?, ?)";
    
    private static final String EXAMPLE_DELETE_SQL = "DELETE FROM ds_0.tb_0 WHERE id=?";
    
    private static final String EXAMPLE_UPDATE_SQL = "UPDATE ds_1.tb_1 SET value=? where id=?";
    
    private static final String EXPECT_EMPTY_SQL_DEFINITION = "{\"policy\":\"ForwardRecovery\",\"requests\":[]}";
    
    private static final String EXPECT_SINGLE_SQL_DEFINITION = "{\"policy\":\"ForwardRecovery\",\"requests\":[{\"id\":\"1\",\"datasource\":\"ds_0\",\"type\":\"sql\",\"transaction\""
            + ":{\"sql\":\"INSERT INTO TABLE ds_0.tb_0 (id, value) VALUES (?, ?)\",\"params\":[[1,\"xxx\"]],\"retries\":5},\"compensation\""
            + ":{\"sql\":\"DELETE FROM ds_0.tb_0 WHERE id=?\",\"params\":[[1]],\"retries\":5},\"parents\":[],\"failRetryDelayMilliseconds\":5000}]}";
    
    private static final String EXPECT_DOUBLE_SQL_DEFINITION = "{\"policy\":\"ForwardRecovery\",\"requests\":[{\"id\":\"1\",\"datasource\":\"ds_0\",\"type\":\"sql\",\"transaction\""
            + ":{\"sql\":\"INSERT INTO TABLE ds_0.tb_0 (id, value) VALUES (?, ?)\",\"params\":[[1,\"xxx\"]],\"retries\":5},\"compensation\""
            + ":{\"sql\":\"DELETE FROM ds_0.tb_0 WHERE id=?\",\"params\":[[1]],\"retries\":5},\"parents\":[],\"failRetryDelayMilliseconds\":5000},{\"id\":\"2\",\"datasource\":\"ds_1\","
            + "\"type\":\"sql\",\"transaction\":{\"sql\":\"UPDATE ds_1.tb_1 SET value=? where id=?\",\"params\":[[\"yyy\",2]],\"retries\":5},\"compensation\""
            + ":{\"sql\":\"UPDATE ds_1.tb_1 SET value=? where id=?\",\"params\":[[\"xxx\",2]],\"retries\":5},\"parents\":[],\"failRetryDelayMilliseconds\":5000}]}";
    
    private static final String EXPECT_PARENTS_SQL_DEFINITION = "{\"policy\":\"ForwardRecovery\",\"requests\":[{\"id\":\"1\",\"datasource\":\"ds_0\",\"type\":\"sql\",\"transaction\""
            + ":{\"sql\":\"INSERT INTO TABLE ds_0.tb_0 (id, value) VALUES (?, ?)\",\"params\":[[1,\"xxx\"]],\"retries\":5},\"compensation\""
            + ":{\"sql\":\"DELETE FROM ds_0.tb_0 WHERE id=?\",\"params\":[[1]],\"retries\":5},\"parents\":[],\"failRetryDelayMilliseconds\":5000}"
            + ",{\"id\":\"2\",\"datasource\":\"ds_1\",\"type\":\"sql\",\"transaction\":{\"sql\":\"UPDATE ds_1.tb_1 SET value=? where id=?\",\"params\":[[\"yyy\",2]],\"retries\":5}"
            + ",\"compensation\":{\"sql\":\"UPDATE ds_1.tb_1 SET value=? where id=?\",\"params\":[[\"xxx\",2]],\"retries\":5},\"parents\":[\"1\"],\"failRetryDelayMilliseconds\":5000}]}";
    
    private static final String EXPECT_ROLLBACK_SQL_DEFINITION = "{\"policy\":\"ForwardRecovery\",\"requests\":[{\"id\":\"rollbackTag\",\"datasource\":\"rollbackTag\",\"type\":\"sql\""
            + ",\"transaction\":{\"sql\":\"rollbackTag\",\"params\":[],\"retries\":5},\"compensation\":{\"sql\":\"rollbackTag\",\"params\":[],\"retries\":5},\"parents\":[]"
            + ",\"failRetryDelayMilliseconds\":5000}]}";
    
    private SagaDefinitionBuilder builder;
    
    @BeforeClass
    public static void setUpClass() {
        INSERT_PARAMETERS.add(Arrays.<Object>asList(1, "xxx"));
        DELETE_PARAMETERS.add(Collections.<Object>singletonList(1));
        UPDATE_PARAMETERS.add(Arrays.<Object>asList("yyy", 2));
        UPDATE_C_PARAMETERS.add(Arrays.<Object>asList("xxx", 2));
    }
    
    @Before
    public void setUp() {
        builder = new SagaDefinitionBuilder(RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY, 5, 5, 5000);
    }
    
    @Test
    public void assertBuildEmpty() throws JsonProcessingException {
        assertThat(builder.build(), is(EXPECT_EMPTY_SQL_DEFINITION));
    }
    
    @Test
    public void assertAddChildRequestAndBuild() throws JsonProcessingException {
        builder.addSagaRequest("1", "ds_0", EXAMPLE_INSERT_SQL, INSERT_PARAMETERS, EXAMPLE_DELETE_SQL, DELETE_PARAMETERS);
        assertThat(builder.build(), is(EXPECT_SINGLE_SQL_DEFINITION));
        builder.addSagaRequest("2", "ds_1", EXAMPLE_UPDATE_SQL, UPDATE_PARAMETERS, EXAMPLE_UPDATE_SQL, UPDATE_C_PARAMETERS);
        assertThat(builder.build(), is(EXPECT_DOUBLE_SQL_DEFINITION));
    }
    
    @Test
    public void assertSwitchParents() throws JsonProcessingException {
        builder.addSagaRequest("1", "ds_0", EXAMPLE_INSERT_SQL, INSERT_PARAMETERS, EXAMPLE_DELETE_SQL, DELETE_PARAMETERS);
        builder.nextLogicSQL();
        builder.addSagaRequest("2", "ds_1", EXAMPLE_UPDATE_SQL, UPDATE_PARAMETERS, EXAMPLE_UPDATE_SQL, UPDATE_C_PARAMETERS);
        assertThat(builder.build(), is(EXPECT_PARENTS_SQL_DEFINITION));
    }
    
    @Test
    public void assertAddRollbackRequest() throws JsonProcessingException {
        builder.addRollbackRequest();
        assertThat(builder.build(), is(EXPECT_ROLLBACK_SQL_DEFINITION));
    }
}
