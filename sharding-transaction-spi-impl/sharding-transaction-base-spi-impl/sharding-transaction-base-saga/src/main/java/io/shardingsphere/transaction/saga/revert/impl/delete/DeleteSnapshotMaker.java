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

package io.shardingsphere.transaction.saga.revert.impl.delete;

import io.shardingsphere.transaction.saga.revert.api.SnapshotParameter;
import io.shardingsphere.transaction.saga.utils.JDBCUtil;
import org.apache.shardingsphere.core.parse.lexer.token.DefaultKeyword;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Delete snapshot maker.
 *
 * @author duhongjun
 */
public class DeleteSnapshotMaker {
    
    /**
     * Make snapshot before execute delete statement.
     *
     * @param snapshotParameter snapshot paramenter
     * @param keys table keys
     * @return query result
     * @throws SQLException failed to execute SQL, throw this exception
     */
    public List<Map<String, Object>> make(final SnapshotParameter snapshotParameter, final List<String> keys)
            throws SQLException {
        String selectSQL = makeSelectSql(snapshotParameter, keys);
        Collection<Object> selectParams = makeSelectParam(snapshotParameter);
        return JDBCUtil.executeQuery(snapshotParameter.getConnection(), selectSQL, selectParams);
    }
    
    private String makeSelectSql(final SnapshotParameter snapshotParameter, final List<String> keys) {
        StringBuilder builder = new StringBuilder();
        builder.append(DefaultKeyword.SELECT).append(" ");
        fillSelectItem(builder, snapshotParameter, keys);
        builder.append(DefaultKeyword.FROM).append(" ");
        builder.append(snapshotParameter.getActualTable());
        if (!snapshotParameter.getStatement().getUpdateTableAlias().isEmpty()) {
            Map.Entry<String, String> entry = snapshotParameter.getStatement().getUpdateTableAlias().entrySet().iterator().next();
            if (!entry.getKey().equals(entry.getValue())) {
                builder.append(" ").append(entry.getKey()).append(" ");
            }
        }
        if (0 < snapshotParameter.getStatement().getWhereStartIndex()) {
            builder.append(" ").append(
                    snapshotParameter.getLogicSQL().substring(snapshotParameter.getStatement().getWhereStartIndex(),
                            snapshotParameter.getStatement().getWhereStopIndex() + 1));
        }
        return builder.toString();
    }
    
    protected void fillSelectItem(final StringBuilder builder, final SnapshotParameter snapshotParameter, final List<String> keys) {
        builder.append("* ");
    }
    
    private Collection<Object> makeSelectParam(final SnapshotParameter snapshotParameter) throws SQLException {
        if (null == snapshotParameter.getActualSQLParams() || snapshotParameter.getActualSQLParams().size() <= 0) {
            return Collections.emptyList();
        }
        int start = snapshotParameter.getStatement().getWhereParameterStartIndex();
        int stop = snapshotParameter.getStatement().getWhereParameterEndIndex();
        Collection<Object> selectParams = new LinkedList<>();
        for (int i = start; i <= stop; i++) {
            selectParams.add(snapshotParameter.getActualSQLParams().get(i));
        }
        return selectParams;
    }
}
