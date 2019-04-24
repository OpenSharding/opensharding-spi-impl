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

package io.shardingsphere.transaction.base.saga.persistence.jpa;

import io.shardingsphere.transaction.base.saga.persistence.jpa.entity.SagaEventEntity;
import io.shardingsphere.transaction.base.saga.persistence.jpa.repository.SagaEventRepository;
import io.shardingsphere.transaction.saga.core.actuator.transport.ShardingTransportFactory;
import org.apache.servicecomb.saga.core.EventEnvelope;
import org.apache.servicecomb.saga.core.JacksonToJsonFormat;
import org.apache.servicecomb.saga.core.PersistentStore;
import org.apache.servicecomb.saga.core.SagaEvent;
import org.apache.servicecomb.saga.core.ToJsonFormat;
import org.apache.servicecomb.saga.format.JacksonSagaEventFormat;
import org.apache.servicecomb.saga.format.SagaEventFormat;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Saga persistence implement.
 *
 * @author yangyi
 */
public final class SagaPersistenceImpl implements PersistentStore {
    
    private final SagaEventRepository sagaEventRepository = new SagaEventRepository();
    
    private final SagaEventFormat sagaEventFormat = new JacksonSagaEventFormat(ShardingTransportFactory.getInstance());
    
    private final ToJsonFormat toJsonFormat = new JacksonToJsonFormat();
    
    @Override
    public Map<String, List<EventEnvelope>> findPendingSagaEvents() {
        List<SagaEventEntity> events = sagaEventRepository.findIncompleteSagaEventsGroupBySagaId();
    
        Map<String, List<EventEnvelope>> result = new HashMap<>();
        for (SagaEventEntity each : events) {
            if (!result.containsKey(each.getSagaId())) {
                result.put(each.getSagaId(), new LinkedList<EventEnvelope>());
            }
            result.get(each.getSagaId()).add(new EventEnvelope(each.getId(), each.getCreationTime().getTime(),
                                             sagaEventFormat.toSagaEvent(each.getSagaId(), each.getType(), each.getContentJson())));
        }
    
        return result;
    }
    
    @Override
    public void offer(final SagaEvent sagaEvent) {
        SagaEventEntity eventEntity = new SagaEventEntity();
        eventEntity.setSagaId(sagaEvent.sagaId);
        eventEntity.setType(sagaEvent.getClass().getSimpleName());
        eventEntity.setContentJson(sagaEvent.json(toJsonFormat));
        sagaEventRepository.insert(eventEntity);
    }
}
