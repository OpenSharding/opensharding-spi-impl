/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.saga;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public final class SagaBranchTransactionTest {
    
    private String ds = "ds";
    
    private String sql = "sql";
    
    @Test
    public void assertHashCode() {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction(ds, sql, getStringParams());
        assertTrue(new SagaBranchTransaction(ds, sql, getStringParams()).hashCode() == sagaBranchTransaction.hashCode());
        assertTrue(new SagaBranchTransaction(ds, sql, getMixedParams()).hashCode() == sagaBranchTransaction.hashCode());
    }
    
    @Test
    public void assertEquals() {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction(ds, sql, getStringParams());
        assertTrue(sagaBranchTransaction.equals(new SagaBranchTransaction(ds, sql, getStringParams())));
        assertTrue(sagaBranchTransaction.equals(new SagaBranchTransaction(ds, sql, getMixedParams())));
    }
    
    @Test
    public void assertToString() {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction(ds, sql, getStringParams());
        assertTrue(new SagaBranchTransaction(ds, sql, getStringParams()).toString().equals(sagaBranchTransaction.toString()));
        assertTrue(new SagaBranchTransaction(ds, sql, getMixedParams()).toString().equals(sagaBranchTransaction.toString()));
    }
    
    private List<List<Object>> getStringParams() {
        List<List<Object>> result = new ArrayList<>();
        List<Object> param = new ArrayList<>();
        param.add("1");
        param.add("x");
        result.add(param);
        param = new ArrayList<>();
        param.add("2");
        param.add("y");
        return result;
    }
    
    private List<List<Object>> getMixedParams() {
        List<List<Object>> result = new ArrayList<>();
        List<Object> param = new ArrayList<>();
        param.add(1);
        param.add("x");
        result.add(param);
        param = new ArrayList<>();
        param.add(2);
        param.add("y");
        return result;
    }
}
