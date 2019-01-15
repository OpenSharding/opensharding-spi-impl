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

package io.shardingsphere.transaction.saga.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedNativeQuery;
import javax.persistence.Table;
import java.util.Date;

/**
 * Saga event entity.
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
    
    @Column(name = "content_json", columnDefinition = "TEXT")
    private String contentJson;
}
