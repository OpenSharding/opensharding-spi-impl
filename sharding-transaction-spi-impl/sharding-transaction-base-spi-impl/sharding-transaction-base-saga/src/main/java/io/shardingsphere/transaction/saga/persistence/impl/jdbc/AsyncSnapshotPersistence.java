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
import com.google.common.util.concurrent.MoreExecutors;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.core.executor.ShardingThreadFactoryBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Asynchronous saga snapshot persistence.
 *
 * @author yangyi
 */
@Slf4j
public class AsyncSnapshotPersistence {
    
    private static final String DELETE_SQL = "DELETE FROM saga_snapshot WHERE transaction_id = ?";
    
    private static final List<Collection<String>> COMMITTED_TRANSACTION_ID_BUFFER = Lists.newArrayList();
    
    private final DataSource dataSource;
    
    private ScheduledExecutorService timerExecutor;
    
    public AsyncSnapshotPersistence(final DataSource dataSource) {
        this.dataSource = dataSource;
        COMMITTED_TRANSACTION_ID_BUFFER.add(Lists.<String>newArrayList());
        timerExecutor = Executors.newSingleThreadScheduledExecutor(ShardingThreadFactoryBuilder.build("AsyncPersistence"));
        MoreExecutors.addDelayedShutdownHook(timerExecutor, 30, TimeUnit.SECONDS);
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                deleteCommittedSnapshots();
            }
        }, 10, 1000, TimeUnit.MILLISECONDS);
    }
    
    private void deleteCommittedSnapshots() {
        Collection<String> transactionIds;
        synchronized (COMMITTED_TRANSACTION_ID_BUFFER) {
            if (COMMITTED_TRANSACTION_ID_BUFFER.get(0).size() == 0) {
                return;
            }
            transactionIds = COMMITTED_TRANSACTION_ID_BUFFER.remove(0);
            COMMITTED_TRANSACTION_ID_BUFFER.add(Lists.<String>newArrayList());
        }
        List<Collection<Object>> committedTransactionIds = Lists.newArrayList();
        for (String transactionId : transactionIds) {
            committedTransactionIds.add(Lists.<Object>newArrayList(transactionId));
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            JDBCUtil.executeBatch(connection, DELETE_SQL, committedTransactionIds);
            connection.commit();
        } catch (SQLException ex) {
            log.warn("Delete saga snapshot failed", ex);
        }
    }
    
    /**
     * Delete all snapshots by transaction id.
     *
     * @param transactionId transaction id
     */
    public void delete(final String transactionId) {
        synchronized (COMMITTED_TRANSACTION_ID_BUFFER) {
            COMMITTED_TRANSACTION_ID_BUFFER.get(0).add(transactionId);
        }
    }
}
