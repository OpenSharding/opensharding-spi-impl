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

package io.opensharding.orchestration.reg.zookeeper.natived.client.retry;

import io.opensharding.orchestration.reg.zookeeper.natived.client.zookeeper.base.BaseOperation;

import java.util.concurrent.DelayQueue;

/**
 * Async retry center.
 *
 * @author lidongbo
 */
public enum AsyncRetryCenter {
    
    INSTANCE;
    
    private final DelayQueue<BaseOperation> queue = new DelayQueue<>();
    
    private final RetryThread retryThread = new RetryThread(queue);
    
    private boolean started;
    
    private DelayRetryPolicy delayRetryPolicy;
    
    /**
     * Initialize.
     *
     * @param delayRetryPolicy delay retry policy
     */
    public void init(final DelayRetryPolicy delayRetryPolicy) {
        this.delayRetryPolicy = null == delayRetryPolicy ? DelayRetryPolicy.defaultDelayPolicy() : delayRetryPolicy;
    }
    
    /**
     * start.
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        retryThread.setName("retry-thread");
        retryThread.start();
        started = true;
    }
    
    /**
     * add async operation.
     *
     * @param operation operation
     */
    public void add(final BaseOperation operation) {
        if (null == delayRetryPolicy) {
            delayRetryPolicy = DelayRetryPolicy.defaultDelayPolicy();
        }
        operation.setDelayPolicyExecutor(new DelayPolicyExecutor(delayRetryPolicy));
        queue.offer(operation);
    }
}
