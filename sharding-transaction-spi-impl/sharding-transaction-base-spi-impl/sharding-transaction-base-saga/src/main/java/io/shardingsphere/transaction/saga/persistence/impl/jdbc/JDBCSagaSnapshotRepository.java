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

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import io.shardingsphere.transaction.saga.persistence.SagaSnapshot;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC saga snapshot repository.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Slf4j
public final class JDBCSagaSnapshotRepository implements TableCreator {
    
    private static final String SNAPSHOT_CREATE_TRANSACTION_ID_INDEX_SQL = "CREATE INDEX IF NOT EXISTS transaction_id_index ON saga_snapshot(transaction_id)";
    
    private static final String SNAPSHOT_CREATE_SNAPSHOT_ID_INDEX_SQL = "CREATE INDEX IF NOT EXISTS snapshot_id_index ON saga_snapshot(snapshot_id)";
    
    private static final String INSERT_SQL = "INSERT INTO saga_snapshot (transaction_id, snapshot_id, transaction_context, revert_context) values (?, ?, ?, ?)";
    
    private static final String DELETE_SQL = "DELETE FROM saga_snapshot WHERE transaction_id = ?";
    
    private final DataSource dataSource;
    
    private final DatabaseType databaseType;
    
    @Override
    public void createTableIfNotExists() {
        SnapshotCreateTableSQL createTableSQL = new SnapshotCreateTableSQL();
        try (Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableSQL.getCreateTableSQL(databaseType));
            createIndex(statement);
        } catch (SQLException ex) {
            throw new ShardingException("Create saga snapshot persistence table failed", ex);
        }
    }
    
    private void createIndex(final Statement statement) throws SQLException {
        if (DatabaseType.MySQL != databaseType) {
            statement.executeUpdate(SNAPSHOT_CREATE_TRANSACTION_ID_INDEX_SQL);
            statement.executeUpdate(SNAPSHOT_CREATE_SNAPSHOT_ID_INDEX_SQL);
        }
    }
    
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
            statement.setObject(3, sagaSnapshot.getTransactionContext().toString());
            statement.setObject(4, sagaSnapshot.getRevertContext().toString());
            statement.executeUpdate();
        } catch (SQLException ex) {
            log.warn("Persist saga snapshot failed", ex);
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
