package io.shardingsphere.transaction.saga.persistence.repository;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import io.shardingsphere.transaction.saga.persistence.entity.SagaEventEntity;

/**
 * Saga event repository
 *
 * @author yangyi
 */
public class SagaEventRepository {
    
    private final EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("io.shardingsphere.transaction.saga.persistence");
    
    private final EntityManager entityManager;
    
    public SagaEventRepository() {
        this.entityManager = entityManagerFactory.createEntityManager();
    }
    
    public void insert(final SagaEventEntity sagaEventEntity) {
        entityManager.getTransaction().begin();
        entityManager.persist(sagaEventEntity);
        entityManager.getTransaction().commit();
    }
    
    public List<SagaEventEntity> findIncompleteSagaEventsGroupBySagaId() {
        return entityManager.createNamedQuery("findIncompleteSagaEventsGroupBySagaId", SagaEventEntity.class).getResultList();
    }
    
    public void deleteBySagaId(final String sagaId) {
        entityManager.getTransaction().begin();
        entityManager.createNativeQuery("DELETE FROM saga_event WHERE saga_id = ?").setParameter(1, sagaId).executeUpdate();
        entityManager.getTransaction().commit();
    }
}
