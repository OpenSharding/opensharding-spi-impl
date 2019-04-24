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

package io.shardingsphere.transaction.base.saga.actuator.definition;

import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class SagaDefinitionTest {
    
    @Mock
    private SagaSQLUnit sagaSQLUnit;
    
    private List<SagaRequest> sagaRequests = new LinkedList<>();
    
    @Before
    public void setUp() {
        SagaRequest sagaRequest = new SagaRequest("id", "ds", "sql", sagaSQLUnit, sagaSQLUnit, Collections.<String>emptyList(), 10);
        sagaRequests.add(sagaRequest);
    }
    
    @Test
    public void assertToJson() {
        SagaDefinition sagaDefinition = new SagaDefinition(RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY, sagaRequests);
        String actual = sagaDefinition.toJson();
        String expected = "{\"policy\":\"BackwardRecovery\",\"requests\":[{\"id\":\"id\",\"datasource\":\"ds\",\"type\":\"sql\",\"transaction\":{\"sql\":null,\"params\":[]," +
            "\"retries\":0},\"compensation\":{\"sql\":null,\"params\":[],\"retries\":0},\"parents\":[],\"failRetryDelayMilliseconds\":10}]}";
        assertThat(actual, is(expected));
    }
}