package io.shardingsphere.transaction.saga.persistence.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Saga snapshot entity
 *
 * @author yangyi
 */
@Entity
@Table(name = "saga_snapshot",
       indexes = {@Index(name = "transaction_id", columnList = "transaction_id"),
                  @Index(name = "snapshot_id", columnList = "snapshot_id")}
)
@NamedNativeQueries(value = @NamedNativeQuery(name = "selectByTransactionId", query = "SELECT * FROM saga_snapshot WHERE transaction_id=?", resultClass = SagaSnapshotEntity.class))
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class SagaSnapshotEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "transaction_id")
    private String transactionId;
    
    @Column(name = "snapshot_id")
    private Integer snapshotId;
    
    @Column(name = "transaction_context")
    private String transactionContext;
    
    @Column(name = "revert_context")
    private String revertContext;
    
    @Column(name = "executeStatus")
    private String executeStatus;
}
