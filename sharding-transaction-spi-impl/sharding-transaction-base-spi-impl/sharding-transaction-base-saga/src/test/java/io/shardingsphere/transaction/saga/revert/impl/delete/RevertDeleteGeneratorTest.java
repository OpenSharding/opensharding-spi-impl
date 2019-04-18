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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.shardingsphere.transaction.saga.revert.api.RevertSQLUnit;
import io.shardingsphere.transaction.saga.revert.util.SnapshotUtil;
import io.shardingsphere.transaction.saga.revert.util.TableMetaDataUtil;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RevertDeleteGeneratorTest {
    
//    @Test
//    public void assertGenerate() throws Exception {
//        DeleteRevertSQLGenerator revertDeleteGenerator = new DeleteRevertSQLGenerator();
//        Optional<RevertSQLUnit> revertContext = revertDeleteGenerator.generate(new DeleteRevertSQLContext(
//            TableMetaDataUtil.ACTUAL_TABLE_NAME, SnapshotUtil.getSnapshot()));
//        assertTrue(revertContext.isPresent());
//        assertThat(revertContext.get().getRevertSQL(), is("INSERT INTO t_order_1 VALUES (?,?,?)"));
//        assertThat(revertContext.get().getRevertParams().size(), is(1));
//        assertThat(revertContext.get().getRevertParams().get(0).size(), is(3));
//        SnapshotUtil.assertSnapshot(revertContext.get().getRevertParams().get(0).iterator());
//    }
//
//    @Test
//    public void assertGenerateWithEmptyParameters() {
//        DeleteRevertSQLGenerator revertDeleteGenerator = new DeleteRevertSQLGenerator();
//        Optional<RevertSQLUnit> revertContext = revertDeleteGenerator.generate(new DeleteRevertSQLContext(TableMetaDataUtil.ACTUAL_TABLE_NAME, Lists.<Map<String, Object>>newArrayList()));
//        assertFalse(revertContext.isPresent());
//    }
}
