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

package io.shardingsphere.transaction.saga.revert;

import com.google.common.base.Optional;

import java.sql.SQLException;

/**
 * Revert operate.
 *
 * @author duhongjun
 */
public interface RevertOperate {
    
    /**
     * Generate snapshot.
     * 
     * @param snapshotParameter snapshot parameter
     *
     * @return revert result
     * @throws SQLException when failed to get snapshot throw exception
     */
    Optional<SQLRevertResult> snapshot(SnapshotParameter snapshotParameter) throws SQLException;
    
    /**
     *  Revert data due to snapshot.
     *
     * @param revertParameter revert parameter
     * @throws SQLException when failed to execute sql throw exception
     */
    void revert(RevertParameter revertParameter) throws SQLException;
}
