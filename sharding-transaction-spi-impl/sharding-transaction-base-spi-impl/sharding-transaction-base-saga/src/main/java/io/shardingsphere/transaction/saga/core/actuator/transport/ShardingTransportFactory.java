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

package io.shardingsphere.transaction.saga.core.actuator.transport;

import io.shardingsphere.transaction.saga.core.SagaTransactionHolder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.servicecomb.saga.transports.SQLTransport;
import org.apache.servicecomb.saga.transports.TransportFactory;

/**
 * Sharding transport factory for service comb saga {@code TransportFactory}.
 *
 * @author yangyi
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ShardingTransportFactory implements TransportFactory<SQLTransport> {
    
    private static final ShardingTransportFactory INSTANCE = new ShardingTransportFactory();
    
    /**
     * Get instance of sharding transport factory.
     *
     * @return instance of sharding transport factory
     */
    public static ShardingTransportFactory getInstance() {
        return INSTANCE;
    }
    
    @Override
    public SQLTransport getTransport() {
        return new ShardingSQLTransport(SagaTransactionHolder.get());
    }
}
