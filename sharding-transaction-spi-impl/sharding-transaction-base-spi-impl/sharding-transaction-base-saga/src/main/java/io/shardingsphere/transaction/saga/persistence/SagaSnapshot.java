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

package io.shardingsphere.transaction.saga.persistence;

import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.revert.engine.RevertSQLResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Saga snapshot bean.
 *
 * @author yangyi
 */
@RequiredArgsConstructor
@Getter
public final class SagaSnapshot {
    
    private final String transactionId;
    
    private final int snapshotId;
    
    private final SagaBranchTransaction transactionContext;
    
    private final RevertSQLResult revertContext;
}
