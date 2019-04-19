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

package io.shardingsphere.transaction.saga.revert.impl.insert;

public class RevertInsertGeneratorTest {
    
//    private InsertRevertSQLContext insertGeneratorParameter;
//
//    @Test
//    public void generateWithKeyGenerator() {
//        mockInsertStatementWithKeyGenerator();
//        RevertInsertSQLGenerator insertGenerator = new RevertInsertSQLGenerator();
//        Optional<RevertSQLUnit> revertContext = insertGenerator.generate(insertGeneratorParameter);
//        assertTrue(revertContext.isPresent());
//        assertThat(revertContext.get().getRevertSQL(), is("DELETE FROM t_order_1 WHERE order_id = ?"));
//        assertThat(revertContext.get().getRevertParams().size(), is(1));
//        assertThat(revertContext.get().getRevertParams().get(0).size(), is(1));
//        Iterator iterator = revertContext.get().getRevertParams().get(0).iterator();
//        assertThat((long) iterator.next(), equalTo(TableMetaDataUtil.ORDER_ID_VALUE));
//    }
//
//    private void mockInsertStatementWithKeyGenerator() {
//        List<String> columnNames = Lists.newArrayList();
//        columnNames.add(TableMetaDataUtil.COLUMN_USER_ID);
//        columnNames.add(TableMetaDataUtil.COLUMN_STATUS);
//        insertGeneratorParameter = new InsertRevertSQLContext(TableMetaDataUtil.ACTUAL_TABLE_NAME, columnNames,
//            TableMetaDataUtil.KEYS, Lists.<Object>newArrayList(TableMetaDataUtil.USER_ID_VALUE, TableMetaDataUtil.STATUS_VALUE, TableMetaDataUtil.ORDER_ID_VALUE), 1, true);
//    }
//
//    @Test
//    public void generateWithoutKeyGenerator() {
//        mockInsertStatementWithoutKeyGenerator();
//        RevertInsertSQLGenerator insertGenerator = new RevertInsertSQLGenerator();
//        Optional<RevertSQLUnit> revertContext = insertGenerator.generate(insertGeneratorParameter);
//        assertTrue(revertContext.isPresent());
//        assertThat(revertContext.get().getRevertSQL(), is("DELETE FROM t_order_1 WHERE order_id = ?"));
//        assertThat(revertContext.get().getRevertParams().size(), is(1));
//        assertThat(revertContext.get().getRevertParams().get(0).size(), is(1));
//        Iterator iterator = revertContext.get().getRevertParams().get(0).iterator();
//        assertThat((long) iterator.next(), equalTo(1L));
//    }
//
//    private void mockInsertStatementWithoutKeyGenerator() {
//        Map<String, Object> keyValue = new HashMap<>();
//        List<String> columnNames = Lists.newArrayList();
//        columnNames.add(TableMetaDataUtil.COLUMN_ORDER_ID);
//        keyValue.put(TableMetaDataUtil.COLUMN_ORDER_ID, 1L);
//        columnNames.add(TableMetaDataUtil.COLUMN_USER_ID);
//        columnNames.add(TableMetaDataUtil.COLUMN_STATUS);
//        insertGeneratorParameter = new InsertRevertSQLContext(TableMetaDataUtil.ACTUAL_TABLE_NAME, columnNames,
//            TableMetaDataUtil.KEYS, Lists.<Object>newArrayList(TableMetaDataUtil.ORDER_ID_VALUE, TableMetaDataUtil.USER_ID_VALUE, TableMetaDataUtil.STATUS_VALUE), 1, false);
//        insertGeneratorParameter.getInvertValues().add(keyValue);
//    }
}
