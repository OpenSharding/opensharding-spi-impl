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

import java.sql.SQLException;
import java.util.List;

/**
 * Revert engine.
 *
 * @author yangyi
 */
public interface RevertEngine {
    
    /**
     * Get revert result.
     *
     * @param datasourceName data source name
     * @param sql execute SQL
     * @param parameters SQL parameters
     * @return revert result
     * @throws SQLException SQL exception
     */
    RevertResult revert(String datasourceName, String sql, List<List<Object>> parameters) throws SQLException;
    
    /**
     * Get revert result.
     *
     * @param datasourceName data source name
     * @param sql execute SQL
     * @param parameters SQL parameters
     * @return revert result
     * @throws SQLException SQL exception
     */
    RevertResult revert(String datasourceName, String sql, Object[] parameters) throws SQLException;
    
}
