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

package io.shardingsphere.transaction.saga.resource;

import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.revert.SQLRevertEngine;
import lombok.Getter;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Resources for saga transaction.
 *
 * @author yangyi
 */
@Getter
public class SagaTransactionResource {
    
    private final SagaPersistence persistence;
    
    private final SQLRevertEngine revertEngine;
    
    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();
    
    public SagaTransactionResource(final SagaPersistence sagaPersistence) {
        this.persistence = sagaPersistence;
        this.revertEngine = new SQLRevertEngine(connections);
    }
    
}
