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

package io.shardingsphere.transaction.spring.boot;

import io.shardingsphere.transaction.aspect.ShardingTransactionJDBCAspect;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.transaction.TransactionManagerCustomizers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Spring boot sharding transaction configuration.
 *
 * @author yangyi
 */
@Configuration
@EnableAspectJAutoProxy(proxyTargetClass = true)
@NoArgsConstructor
public class ShardingTransactionJDBCConfiguration {
    
    /**
     * Build sharding transaction aspect bean.
     *
     * @return sharding transaction aspect bean
     */
    @Bean
    public ShardingTransactionJDBCAspect shardingTransactionalAspect() {
        return new ShardingTransactionJDBCAspect();
    }
    
    /**
     * Build hibernate transaction manager.
     *
     * @param transactionManagerCustomizers transaction manager customizers
     * @return jpa transaction manager
     */
    @Bean
    @ConditionalOnMissingBean(PlatformTransactionManager.class)
    @ConditionalOnClass(value = LocalContainerEntityManagerFactoryBean.class, name = "javax.persistence.EntityManager")
    public PlatformTransactionManager jpaTransactionManager(final ObjectProvider<TransactionManagerCustomizers> transactionManagerCustomizers) {
        JpaTransactionManager result = new JpaTransactionManager();
        if (null != transactionManagerCustomizers.getIfAvailable()) {
            transactionManagerCustomizers.getIfAvailable().customize(result);
        }
        return result;
    }
    
    /**
     * Build datasource transaction manager.
     *
     * @param dataSource data source
     * @param transactionManagerCustomizers transaction manager customizers
     * @return datasource transaction manager
     */
    @Bean
    @ConditionalOnMissingBean(PlatformTransactionManager.class)
    public PlatformTransactionManager dataSourceTransactionManager(final DataSource dataSource, final ObjectProvider<TransactionManagerCustomizers> transactionManagerCustomizers) {
        DataSourceTransactionManager result = new DataSourceTransactionManager(dataSource);
        if (null != transactionManagerCustomizers.getIfAvailable()) {
            transactionManagerCustomizers.getIfAvailable().customize(result);
        }
        return result;
    }
}
