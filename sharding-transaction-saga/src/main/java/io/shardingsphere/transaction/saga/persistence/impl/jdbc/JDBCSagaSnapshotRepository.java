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

package io.shardingsphere.transaction.saga.persistence.impl.jdbc;

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * JDBC saga snapshot repository.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Slf4j
public final class JDBCSagaSnapshotRepository {
    
    private static final String INSERT_SQL = "INSERT INTO saga_snapshot (transaction_id, snapshot_id, transaction_context, revert_context, execute_status) values (?, ?, ?, ?, ?)";
    
    private static final String UPDATE_SQL = "UPDATE saga_snapshot SET execute_status = ? WHERE transaction_id = ? AND snapshot_id = ?";
    
    private static final String DELETE_SQL = "DELETE FROM saga_snapshot WHERE transaction_id = ?";
    
    private final DataSource dataSource;
    
    /**
     * Insert new saga snapshot.
     *
     * @param sagaSnapshot saga snapshot
     */
    public void insert(final SagaSnapshot sagaSnapshot) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            statement.setObject(1, sagaSnapshot.getTransactionId());
            statement.setObject(2, sagaSnapshot.getSnapshotId());
            statement.setObject(3, sagaSnapshot.getTransactionContext());
            statement.setObject(4, sagaSnapshot.getRevertContext());
            statement.setObject(5, sagaSnapshot.getExecuteStatus().name());
            statement.executeUpdate();
        } catch (SQLException ex) {
            log.warn("Persist saga snapshot failed", ex);
        }
    }
    
    /**
     * Update execute status for snapshot.
     *
     * @param transactionId transaction id
     * @param snapshotId snapshot id
     * @param executeStatus execute status
     */
    public void update(final String transactionId, final int snapshotId, final ExecuteStatus executeStatus) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(UPDATE_SQL)) {
            statement.setObject(1, transactionId);
            statement.setObject(2, snapshotId);
            statement.setObject(3, executeStatus.name());
            statement.executeUpdate();
        } catch (SQLException ex) {
            log.warn("Update saga snapshot failed", ex);
        }
    }
    
    /**
     * Delete all snapshots by transaction id.
     *
     * @param transactionId transaction id
     */
    public void delete(final String transactionId) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(DELETE_SQL)) {
            statement.setObject(1, transactionId);
            statement.executeUpdate();
        } catch (SQLException ex) {
            log.warn("Delete saga snapshot failed", ex);
        }
    }
}
