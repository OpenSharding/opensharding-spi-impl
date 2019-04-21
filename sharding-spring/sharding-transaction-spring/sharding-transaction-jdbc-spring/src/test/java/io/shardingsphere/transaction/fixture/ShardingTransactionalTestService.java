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

package io.shardingsphere.transaction.fixture;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.shardingsphere.transaction.annotation.ShardingTransactionType;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
@Component
@ShardingTransactionType(TransactionType.XA)
public class ShardingTransactionalTestService {
    
    @ShardingTransactionType
    public void testChangeTransactionTypeToLOCAL() {
        assertThat(TransactionTypeHolder.get(), is(TransactionType.LOCAL));
    }
    
    @ShardingTransactionType(TransactionType.XA)
    public void testChangeTransactionTypeToXA() {
        assertThat(TransactionTypeHolder.get(), is(TransactionType.XA));
    }
    
    @ShardingTransactionType(TransactionType.BASE)
    public void testChangeTransactionTypeToBASE() {
        assertThat(TransactionTypeHolder.get(), is(TransactionType.BASE));
    }
    
    public void testChangeTransactionTypeInClass() {
        assertThat(TransactionTypeHolder.get(), is(TransactionType.XA));
    }
}
