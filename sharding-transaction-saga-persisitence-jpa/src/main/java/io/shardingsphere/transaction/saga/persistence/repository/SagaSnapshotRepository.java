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
    
    private final EntityManager entityManager;
    
    public SagaSnapshotRepository() {
        entityManager = entityManagerFactory.createEntityManager();
    }
    
    public void insert(final SagaSnapshotEntity sagaSnapshotEntity) {
        entityManager.getTransaction().begin();
        entityManager.persist(sagaSnapshotEntity);
        entityManager.getTransaction().commit();
    }
    
    public SagaSnapshotEntity select(final Long id) {
        return entityManager.find(SagaSnapshotEntity.class, id);
    }
    
    public List<SagaSnapshotEntity> selectByTranscationId(final String transactionId) {
        return entityManager.createNamedQuery("selectByTransactionId", SagaSnapshotEntity.class).setParameter(1, transactionId).getResultList();
    }
    
    public void update(final SagaSnapshotEntity sagaSnapshotEntity) {
        entityManager.getTransaction().begin();
        entityManager.merge(sagaSnapshotEntity);
        entityManager.getTransaction().commit();
    }
    
    public void delete(final SagaSnapshotEntity sagaSnapshotEntity) {
        entityManager.remove(sagaSnapshotEntity);
    }
    
    public void deleteByTransactionId(final String transactionId) {
        entityManager.getTransaction().begin();
        entityManager.createNativeQuery("DELETE FROM saga_snapshot WHERE transaction_id=?").setParameter(1, transactionId).executeUpdate();
        entityManager.getTransaction().commit();
    }
    
    public void close() {
        entityManagerFactory.close();
    }
}
