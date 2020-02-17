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

package io.opensharding.transaction.aspect;

import org.apache.shardingsphere.transaction.annotation.ShardingTransactionType;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Sharding transaction JDBC aspect.
 *
 * @author yangyi
 */
@Aspect
@Component
@Order(Ordered.LOWEST_PRECEDENCE - 1)
public class ShardingTransactionJDBCAspect {
    
    /**
     * Sharding transactional AOP pointcut.
     */
    @Pointcut("@annotation(org.apache.shardingsphere.transaction.annotation.ShardingTransactionType) || @within(org.apache.shardingsphere.transaction.annotation.ShardingTransactionType)")
    public void shardingTransactionalJDBCPointCut() {
    }
    
    /**
     * Set transaction type before transaction begin.
     *
     * @param joinPoint join point
     */
    @Before(value = "shardingTransactionalJDBCPointCut()")
    public void setTransactionTypeBeforeTransaction(final JoinPoint joinPoint) {
        ShardingTransactionType shardingTransactionType = getAnnotation(joinPoint);
        TransactionTypeHolder.set(shardingTransactionType.value());
    }
    
    private ShardingTransactionType getAnnotation(final JoinPoint joinPoint) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        ShardingTransactionType result = method.getAnnotation(ShardingTransactionType.class);
        if (null == result) {
            result = method.getDeclaringClass().getAnnotation(ShardingTransactionType.class);
        }
        return result;
    }
    
    /**
     * Clean transaction type after transaction commit/rollback.
     *
     * @param joinPoint join point
     */
    @After(value = "shardingTransactionalJDBCPointCut()")
    public void cleanTransactionTypeAfterTransaction(final JoinPoint joinPoint) {
        TransactionTypeHolder.clear();
    }
}
