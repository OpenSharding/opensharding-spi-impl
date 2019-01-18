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

package io.shardingsphere.transaction.saga.persistence.repository;

import io.shardingsphere.transaction.saga.persistence.entity.SagaEventEntity;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.List;

/**
 * Saga event repository.
 *
 * @author yangyi
 */
public class SagaEventRepository {
    
    private final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("io.shardingsphere.transaction.saga.persistence");
    
    /**
     * Insert new saga event.
     *
     * @param sagaEventEntity saga event entity
     */
    public void insert(final SagaEventEntity sagaEventEntity) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist(sagaEventEntity);
        entityManager.getTransaction().commit();
    }
    
    /**
     * Find Incomplete saga events.
     *
     * @return incomplete saga event list
     */
    public List<SagaEventEntity> findIncompleteSagaEventsGroupBySagaId() {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        return entityManager.createNamedQuery("findIncompleteSagaEventsGroupBySagaId", SagaEventEntity.class).getResultList();
    }
}
