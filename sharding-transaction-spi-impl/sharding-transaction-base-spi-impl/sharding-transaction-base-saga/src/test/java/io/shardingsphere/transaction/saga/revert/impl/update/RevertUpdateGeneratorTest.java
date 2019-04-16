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

package io.shardingsphere.transaction.saga.revert.impl.update;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import io.shardingsphere.transaction.saga.revert.util.SnapshotUtil;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RevertUpdateGeneratorTest {
    
    @Test
    public void assertGenerate() {
        RevertUpdateGenerator revertUpdateGenerator = new RevertUpdateGenerator();
        Optional<RevertSQLUnit> revertContext = revertUpdateGenerator.generate(new RevertUpdateGeneratorParameter(
            TableMetaDataUtil.ACTUAL_TABLE_NAME, SnapshotUtil.getSnapshot(), genUpdateColumns(), TableMetaDataUtil.KEYS, Lists.newArrayList()));
        assertTrue(revertContext.isPresent());
        assertThat(revertContext.get().getRevertSQL(), is("UPDATE t_order_1 SET order_id = ?,user_id = ?,status = ? WHERE order_id = ? "));
        assertThat(revertContext.get().getRevertParams().size(), is(1));
        assertThat(revertContext.get().getRevertParams().get(0).size(), is(4));
        Iterator iterator = revertContext.get().getRevertParams().get(0).iterator();
        SnapshotUtil.assertSnapshot(iterator);
        assertThat((long) iterator.next(), equalTo(TableMetaDataUtil.ORDER_ID_VALUE));
    }
    
    private Map<String, Object> genUpdateColumns() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(TableMetaDataUtil.COLUMN_ORDER_ID, TableMetaDataUtil.ORDER_ID_VALUE);
        result.put(TableMetaDataUtil.COLUMN_USER_ID, TableMetaDataUtil.USER_ID_VALUE);
        result.put(TableMetaDataUtil.COLUMN_STATUS, TableMetaDataUtil.STATUS_VALUE);
        return result;
    }
}
