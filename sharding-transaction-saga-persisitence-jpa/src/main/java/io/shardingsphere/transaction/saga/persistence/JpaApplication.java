package io.shardingsphere.transaction.saga.persistence;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import io.shardingsphere.transaction.saga.persistence.entity.SagaEventEntity;
import io.shardingsphere.transaction.saga.persistence.entity.SagaSnapshotEntity;
import io.shardingsphere.transaction.saga.persistence.repository.SagaEventRepository;
import io.shardingsphere.transaction.saga.persistence.repository.SagaSnapshotRepository;

/**
 * Jpa hibernate saga persistence
 *
 * @author yangyi
 */
public class JpaApplication {
    
    public static void main(String[] args) {
//        snapshot();
        event();
    }
    
    private static void event() {
        SagaEventRepository repository = new SagaEventRepository();
        repository.insert(new SagaEventEntity(null, UUID.randomUUID().toString(), new Date(),  "test", "content"));
        List<SagaEventEntity> list = repository.findIncompleteSagaEventsGroupBySagaId();
        for (SagaEventEntity each : list) {
            System.out.println(each);
            repository.deleteBySagaId(each.getSagaId());
        }
        list = repository.findIncompleteSagaEventsGroupBySagaId();
        for (SagaEventEntity each : list) {
            System.out.println(each);
        }
    }
    
    private static void snapshot() {
        SagaSnapshotRepository repository = new SagaSnapshotRepository();
    
        for (int i = 0; i < 10; i++) {
            String uuid = UUID.randomUUID().toString();
            for (int j = 1; j <= 10; j++) {
                SagaSnapshotEntity sagaSnapshotEntity = new SagaSnapshotEntity();
                sagaSnapshotEntity.setTransactionId(uuid);
                sagaSnapshotEntity.setSnapshotId(j);
                sagaSnapshotEntity.setTransactionContext(String.format(
                    "{\"datasourceName\":\"ds1\",\"sql\":\"insert into t_order_0 (order_id, user_id, status) values (?, ?, ?)\",\"parameters\":[[%d,%d,\"INIT\"]]}", j, j));
                sagaSnapshotEntity.setExecuteStatus("EXECUTION");
                repository.insert(sagaSnapshotEntity);
            }
        
            List<SagaSnapshotEntity> sagaSnapshotEntityList = repository.selectByTranscationId(uuid);
            for (SagaSnapshotEntity each : sagaSnapshotEntityList) {
                System.out.println(each);
                each.setRevertContext("{\"sql\":\"delete from t_order_id where order_id=?\",\"parameters\":[[1]]}");
                each.setExecuteStatus("SUCCESS");
                repository.update(each);
            }
        
            sagaSnapshotEntityList = repository.selectByTranscationId(uuid);
            for (SagaSnapshotEntity each : sagaSnapshotEntityList) {
                System.out.println(each);
            }
        
            repository.deleteByTransactionId(uuid);
        }
    }
}
