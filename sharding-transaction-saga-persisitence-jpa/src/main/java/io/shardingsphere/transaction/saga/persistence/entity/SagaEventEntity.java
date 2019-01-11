package io.shardingsphere.transaction.saga.persistence.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedNativeQuery;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Saga event entity
 *
 * @author yangyi
 */
@Entity
@Table(name = "saga_event")
@NamedNativeQuery(name = "findIncompleteSagaEventsGroupBySagaId",
                  query = "SELECT * FROM saga_event WHERE saga_id NOT IN (SELECT DISTINCT saga_id FROM saga_event WHERE type = 'SagaEndedEvent')",
                  resultClass = SagaEventEntity.class)
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@ToString
public class SagaEventEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "saga_id")
    private String sagaId;
    
    @Column(name = "create_time")
    private Date creationTime;
    
    private String type;
    
    @Column(name = "content_json")
    private String contentJson;
}
