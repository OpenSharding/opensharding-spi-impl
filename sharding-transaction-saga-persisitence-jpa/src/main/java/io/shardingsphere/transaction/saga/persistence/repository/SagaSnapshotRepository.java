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

package io.shardingsphere.transaction.saga.persistence.repository;

import io.shardingsphere.transaction.saga.persistence.entity.SagaSnapshotEntity;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.List;

/**
 * Saga snapshot repository
 *
 * @author yangyi
 */
public class SagaSnapshotRepository {
    
    private final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("io.shardingsphere.transaction.saga.persistence");
    
    public void insert(final SagaSnapshotEntity sagaSnapshotEntity) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist(sagaSnapshotEntity);
        entityManager.getTransaction().commit();
    }
    
    public SagaSnapshotEntity select(final Long id) {
        return entityManagerFactory.createEntityManager().find(SagaSnapshotEntity.class, id);
    }
    
    public List<SagaSnapshotEntity> selectByTranscationId(final String transactionId) {
        return entityManagerFactory.createEntityManager().createNamedQuery("selectByTransactionId", SagaSnapshotEntity.class).setParameter(1, transactionId).getResultList();
    }
    
    public void update(final String transactionId, final int snapshotId, final String executeStatus) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        entityManager.createNativeQuery("UPDATE saga_snapshot SET execute_status = ? WHERE transaction_id=? AND snapshot_id=?")
            .setParameter(1, executeStatus).setParameter(2, transactionId).setParameter(3, snapshotId).executeUpdate();
        entityManager.getTransaction().commit();
    }
    
    public void delete(final SagaSnapshotEntity sagaSnapshotEntity) {
        entityManagerFactory.createEntityManager().remove(sagaSnapshotEntity);
    }
    
    public void deleteByTransactionId(final String transactionId) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        entityManager.createNativeQuery("DELETE FROM saga_snapshot WHERE transaction_id=?").setParameter(1, transactionId).executeUpdate();
        entityManager.getTransaction().commit();
    }
    
    public void close() {
        entityManagerFactory.close();
    }
}
