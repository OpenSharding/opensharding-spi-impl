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
 * Saga snapshot entity.
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
    
    @Column(name = "execute_status")
    private String executeStatus;
}
