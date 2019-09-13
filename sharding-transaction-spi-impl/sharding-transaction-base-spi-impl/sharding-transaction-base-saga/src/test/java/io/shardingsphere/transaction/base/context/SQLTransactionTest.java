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

package io.shardingsphere.transaction.base.context;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class SQLTransactionTest {
    
    private String dataSourceName = "dataSourceName";
    
    private String sql = "sql";
    
    @Test
    public void assertToString() {
        SQLTransaction sqlTransaction = new SQLTransaction(dataSourceName, sql, getStringParameterSets());
        assertThat(new SQLTransaction(dataSourceName, sql, getStringParameterSets()).toString(), is(sqlTransaction.toString()));
        assertThat(new SQLTransaction(dataSourceName, sql, getMixedParameterSets()).toString(), is(sqlTransaction.toString()));
    }
    
    private List<Collection<Object>> getStringParameterSets() {
        List<Collection<Object>> result = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        parameters.add("1");
        parameters.add("x");
        result.add(parameters);
        parameters = new ArrayList<>();
        parameters.add("2");
        parameters.add("y");
        return result;
    }
    
    private List<Collection<Object>> getMixedParameterSets() {
        List<Collection<Object>> result = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        parameters.add(1);
        parameters.add("x");
        result.add(parameters);
        parameters = new ArrayList<>();
        parameters.add(2);
        parameters.add("y");
        return result;
    }
}
