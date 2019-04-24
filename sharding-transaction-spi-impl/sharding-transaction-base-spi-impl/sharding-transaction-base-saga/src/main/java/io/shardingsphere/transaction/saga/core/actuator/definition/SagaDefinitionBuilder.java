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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Saga definition builder.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
public final class SagaDefinitionBuilder {
    
    public static final String ROLLBACK_TAG = "rollbackTag";
    
    private static final String TYPE = "sql";
    
    private final String recoveryPolicy;
    
    private final int transactionMaxRetries;
    
    private final int compensationMaxRetries;
    
    private final int transactionRetryDelayMilliseconds;
    
    private final Collection<SagaRequest> sagaRequests = new LinkedList<>();
    
    private Collection<String> parentsIds;
    
    private Collection<String> requestIds;
    
    /**
     * Switch to next logic SQL.
     */
    public void nextLogicSQL() {
        parentsIds = null == requestIds ? new LinkedList<String>() : new LinkedList<>(requestIds);
        requestIds = new LinkedList<>();
    }
    
    /**
     * Add saga request node to definition graph.
     *
     * @param id request ID
     * @param datasourceName data source name
     * @param sql transaction SQL
     * @param parameterSets transaction SQL parameters
     * @param compensationSQL compensation SQL
     * @param compensationParameters compensation SQL parameters
     */
    public void addSagaRequest(final String id, final String datasourceName, final String sql, final List<List<Object>> parameterSets,
                               final String compensationSQL, final List<Collection<Object>> compensationParameters) {
        Transaction transaction = new Transaction(sql, parameterSets, transactionMaxRetries);
        Compensation compensation = new Compensation(compensationSQL, compensationParameters, compensationMaxRetries);
        sagaRequests.add(new SagaRequest(id, datasourceName, TYPE, transaction, compensation, parentsIds, transactionRetryDelayMilliseconds));
        requestIds.add(id);
    }
    
    /**
     * Add rollback request node to definition graph.
     */
    public void addRollbackRequest() {
        nextLogicSQL();
        addSagaRequest(ROLLBACK_TAG, ROLLBACK_TAG, ROLLBACK_TAG, Lists.<List<Object>>newArrayList(), ROLLBACK_TAG, Lists.<Collection<Object>>newArrayList());
    }
    
    /**
     * Build saga definition json string.
     *
     * @return saga json string
     * @throws JsonProcessingException json process exception
     */
    public String build() throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(new SagaDefinition(recoveryPolicy, sagaRequests));
    }
    
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class SagaDefinition {
        
        private final String policy;
        
        private final Collection<SagaRequest> requests;
    }
    
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class SagaRequest {
        
        private final String id;
        
        private final String datasource;
        
        private final String type;
        
        private final Transaction transaction;
        
        private final Compensation compensation;
        
        private final Collection<String> parents;
        
        private final int failRetryDelayMilliseconds;
    }
    
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class Transaction {
        
        private final String sql;
        
        private final List<List<Object>> params;
        
        private final int retries;
    }
    
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class Compensation {
        
        private final String sql;
        
        private final List<Collection<Object>> params;
        
        private final int retries;
    }
}
