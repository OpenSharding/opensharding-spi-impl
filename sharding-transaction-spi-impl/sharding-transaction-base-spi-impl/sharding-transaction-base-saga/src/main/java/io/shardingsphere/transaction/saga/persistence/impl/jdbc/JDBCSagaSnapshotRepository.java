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

package io.shardingsphere.transaction.saga.persistence.impl.jdbc;

import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * JDBC saga snapshot repository.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Slf4j
public final class JDBCSagaSnapshotRepository {
    
    private static final String INSERT_SQL = "INSERT INTO saga_snapshot (transaction_id, snapshot_id, transaction_context, revert_context) values (?, ?, ?, ?)";
    
    private static final String DELETE_SQL = "DELETE FROM saga_snapshot WHERE transaction_id = ?";
    
    private final DataSource dataSource;
    
    private final AsyncSnapshotPersistence asyncSnapshotPersistence;
    
    /**
     * Insert new saga snapshot.
     *
     * @param sagaSnapshot saga snapshot
     */
    public void insert(final SagaSnapshot sagaSnapshot) {
        try (Connection connection = dataSource.getConnection()) {
            JDBCUtil.executeUpdate(connection, INSERT_SQL, generateParams(sagaSnapshot));
        } catch (SQLException ex) {
            log.warn("Persist saga snapshot failed", ex);
        }
    }
    
    private List<Object> generateParams(final SagaSnapshot sagaSnapshot) {
        List<Object> result = Lists.newArrayList();
        result.add(sagaSnapshot.getTransactionId());
        result.add(sagaSnapshot.getSnapshotId());
        result.add(sagaSnapshot.getTransactionContext().toString());
        result.add(sagaSnapshot.getRevertContext().toString());
        return result;
    }
    
    /**
     * Delete all snapshots by transaction id.
     *
     * @param transactionId transaction id
     */
    public void delete(final String transactionId) {
        asyncSnapshotPersistence.delete(transactionId);
    }
}
