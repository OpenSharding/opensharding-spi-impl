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

package io.opensharding.transaction.base.saga.actuator.transport;

import io.opensharding.transaction.base.saga.ShardingSQLTransactionManager;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.servicecomb.saga.transports.SQLTransport;
import org.apache.servicecomb.saga.transports.TransportFactory;

/**
 * Saga transport factory.
 *
 * @author yangyi
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SagaTransportFactory implements TransportFactory<SQLTransport> {
    
    private static final SagaTransportFactory INSTANCE = new SagaTransportFactory();
    
    /**
     * Get instance of saga transport factory.
     *
     * @return instance of saga transport factory
     */
    public static SagaTransportFactory getInstance() {
        return INSTANCE;
    }
    
    @Override
    public SQLTransport getTransport() {
        return new SagaSQLTransport(ShardingSQLTransactionManager.getInstance().getCurrentTransaction());
    }
}
