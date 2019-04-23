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

package io.shardingsphere.transaction.saga.core.resource.config;

import com.google.common.base.Strings;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.servicecomb.saga.core.RecoveryPolicy;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Saga configuration loader.
 *
 * @author yangyi
 */
@Slf4j
public final class SagaConfigurationLoader {
    
    private static final String CONFIGURATION_FILE = "saga.properties";
    
    private static final String ACTUATOR_PREFIX = "saga.actuator.";
    
    private static final String EXECUTOR_SIZE = ACTUATOR_PREFIX + "executor.size";
    
    private static final String TRANSACTION_MAX_RETRIES = ACTUATOR_PREFIX + "transaction.max.retries";
    
    private static final String COMPENSATION_MAX_RETRIES = ACTUATOR_PREFIX + "compensation.max.retries";
    
    private static final String TRANSACTION_RETRY_DELAY_MILLISECONDS = ACTUATOR_PREFIX + "transaction.retry.delay.milliseconds";
    
    private static final String COMPENSATION_RETRY_DELAY_MILLISECONDS = ACTUATOR_PREFIX + "compensation.retry.delay.milliseconds";
    
    private static final String RECOVERY_POLICY = ACTUATOR_PREFIX + "recovery.policy";
    
    private static final String ENABLED_PERSISTENCE = "saga.persistence.enabled";
    
    private static final String PERSISTENCE_DS_PREFIX = "saga.persistence.ds.";
    
    private static final String URL = PERSISTENCE_DS_PREFIX + "url";
    
    private static final String USERNAME = PERSISTENCE_DS_PREFIX + "username";
    
    private static final String PASSWORD = PERSISTENCE_DS_PREFIX + "password";
    
    private static final String CONNECTION_TIMEOUT_MILLISECONDS = PERSISTENCE_DS_PREFIX + "connection.timeout.milliseconds";
    
    private static final String IDLE_TIMEOUT_MILLISECONDS = PERSISTENCE_DS_PREFIX + "idle.timeout.milliseconds";
    
    private static final String MAINTENANCE_INTERVAL_MILLISECONDS = PERSISTENCE_DS_PREFIX + "maintenance.interval.milliseconds";
    
    private static final String MAX_LIFE_TIME_MILLISECONDS = PERSISTENCE_DS_PREFIX + "max.life.time.milliseconds";
    
    private static final String MAX_POOL_SIZE = PERSISTENCE_DS_PREFIX + "max.pool.size";
    
    private static final String MIN_POOL_SIZE = PERSISTENCE_DS_PREFIX + "min.pool.size";
    
    /**
     * Load saga configuration from properties file.
     *
     * @return saga configuration
     */
    public static SagaConfiguration load() {
        return createSagaConfiguration(loadConfigurationProperties());
    }
    
    @SneakyThrows
    private static Properties loadConfigurationProperties() {
        Properties result = new Properties();
        URL configurationFile = SagaConfigurationLoader.class.getClassLoader().getResource(CONFIGURATION_FILE);
        if (null == configurationFile) {
            log.warn("{} not found at your root classpath, will use default saga configuration", CONFIGURATION_FILE);
            return result;
        }
        result.load(new FileInputStream(new File(configurationFile.getFile())));
        return result;
    }
    
    private static SagaConfiguration createSagaConfiguration(final Properties sagaProperties) {
        SagaConfiguration result = new SagaConfiguration();
        String executorSize = sagaProperties.getProperty(EXECUTOR_SIZE);
        if (!Strings.isNullOrEmpty(executorSize)) {
            result.setExecutorSize(Integer.parseInt(executorSize));
        }
        String transactionMaxRetries = sagaProperties.getProperty(TRANSACTION_MAX_RETRIES);
        if (!Strings.isNullOrEmpty(transactionMaxRetries)) {
            result.setTransactionMaxRetries(Integer.parseInt(transactionMaxRetries));
        }
        String compensationMaxRetries = sagaProperties.getProperty(COMPENSATION_MAX_RETRIES);
        if (!Strings.isNullOrEmpty(compensationMaxRetries)) {
            result.setCompensationMaxRetries(Integer.parseInt(compensationMaxRetries));
        }
        String transactionRetryDelayMilliseconds = sagaProperties.getProperty(TRANSACTION_RETRY_DELAY_MILLISECONDS);
        if (!Strings.isNullOrEmpty(transactionMaxRetries)) {
            result.setTransactionRetryDelayMilliseconds(Integer.parseInt(transactionRetryDelayMilliseconds));
        }
        String compensationRetryDelayMilliseconds = sagaProperties.getProperty(COMPENSATION_RETRY_DELAY_MILLISECONDS);
        if (!Strings.isNullOrEmpty(transactionMaxRetries)) {
            result.setCompensationRetryDelayMilliseconds(Integer.parseInt(compensationRetryDelayMilliseconds));
        }
        String recoveryPolicy = sagaProperties.getProperty(RECOVERY_POLICY);
        if (RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY.equals(recoveryPolicy) || RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(recoveryPolicy)) {
            result.setRecoveryPolicy(recoveryPolicy);
        }
        result.setSagaPersistenceConfiguration(createSagaPersistenceConfiguration(sagaProperties));
        return result;
    }
    
    private static SagaPersistenceConfiguration createSagaPersistenceConfiguration(final Properties sagaProperties) {
        SagaPersistenceConfiguration result = new SagaPersistenceConfiguration();
        String enabledPersistence = sagaProperties.getProperty(ENABLED_PERSISTENCE);
        if (!Strings.isNullOrEmpty(enabledPersistence)) {
            result.setEnablePersistence(Boolean.parseBoolean(enabledPersistence));
        }
        initPersistenceDataSourceProperties(result, sagaProperties);
        return result;
    }
    
    private static void initPersistenceDataSourceProperties(final SagaPersistenceConfiguration result, final Properties sagaProperties) {
        initCommonDataSourceProperties(result, sagaProperties);
        initHikariPoolProperties(result, sagaProperties);
    }
    
    private static void initCommonDataSourceProperties(final SagaPersistenceConfiguration result, final Properties sagaProperties) {
        String url = sagaProperties.getProperty(URL);
        if (null != url) {
            result.setUrl(url);
        }
        String username = sagaProperties.getProperty(USERNAME);
        if (null != username) {
            result.setUsername(username);
        }
        String password = sagaProperties.getProperty(PASSWORD);
        if (null != password) {
            result.setPassword(password);
        }
    }
    
    private static void initHikariPoolProperties(final SagaPersistenceConfiguration result, final Properties sagaProperties) {
        String connectionTimeoutMilliseconds = sagaProperties.getProperty(CONNECTION_TIMEOUT_MILLISECONDS);
        if (!Strings.isNullOrEmpty(connectionTimeoutMilliseconds)) {
            result.setConnectionTimeoutMilliseconds(Long.parseLong(connectionTimeoutMilliseconds));
        }
        String idleTimeoutMilliseconds = sagaProperties.getProperty(IDLE_TIMEOUT_MILLISECONDS);
        if (!Strings.isNullOrEmpty(idleTimeoutMilliseconds)) {
            result.setIdleTimeoutMilliseconds(Long.parseLong(idleTimeoutMilliseconds));
        }
        String maintenanceIntervalMilliseconds = sagaProperties.getProperty(MAINTENANCE_INTERVAL_MILLISECONDS);
        if (!Strings.isNullOrEmpty(maintenanceIntervalMilliseconds)) {
            result.setMaintenanceIntervalMilliseconds(Long.parseLong(maintenanceIntervalMilliseconds));
        }
        String maxLifeTimeMilliseconds = sagaProperties.getProperty(MAX_LIFE_TIME_MILLISECONDS);
        if (!Strings.isNullOrEmpty(maxLifeTimeMilliseconds)) {
            result.setMaxLifetimeMilliseconds(Long.parseLong(maxLifeTimeMilliseconds));
        }
        String maxPoolSize = sagaProperties.getProperty(MAX_POOL_SIZE);
        if (!Strings.isNullOrEmpty(maxPoolSize)) {
            result.setMaxPoolSize(Integer.parseInt(maxPoolSize));
        }
        String minPoolSize = sagaProperties.getProperty(MIN_POOL_SIZE);
        if (!Strings.isNullOrEmpty(minPoolSize)) {
            result.setMinPoolSize(Integer.parseInt(minPoolSize));
        }
    }
}
