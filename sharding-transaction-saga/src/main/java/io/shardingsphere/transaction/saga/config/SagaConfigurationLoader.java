/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.saga.config;

import com.google.common.base.Strings;
import io.shardingsphere.transaction.saga.constant.SagaRecoveryPolicy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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
    
    private static final String PREFIX = "saga.actuator.";
    
    private static final String EXECUTOR_SIZE = PREFIX + "executorSize";
    
    private static final String TRANSACTION_MAX_RETRIES = PREFIX + "transactionMaxRetries";
    
    private static final String COMPENSATION_MAX_RETRIES = PREFIX + "compensationMaxRetries";
    
    private static final String TRANSACTION_RETRY_DELAY = PREFIX + "transactionRetryDelay";
    
    private static final String COMPENSATION_RETRY_DELAY = PREFIX + "compensationRetryDelay";
    
    private static final String RECOVERY_POLICY = PREFIX + "recoveryPolicy";
    
    private static final String ENABLED_PERSISTENCE = "saga.persistence.enabled";
    
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
    
    @SneakyThrows
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
        if (!Strings.isNullOrEmpty(transactionMaxRetries)) {
            result.setCompensationMaxRetries(Integer.parseInt(compensationMaxRetries));
        }
        String transactionRetryDelay = sagaProperties.getProperty(TRANSACTION_RETRY_DELAY);
        if (!Strings.isNullOrEmpty(transactionMaxRetries)) {
            result.setTransactionRetryDelay(Integer.parseInt(transactionRetryDelay));
        }
        String compensationRetryDelay = sagaProperties.getProperty(COMPENSATION_RETRY_DELAY);
        if (!Strings.isNullOrEmpty(transactionMaxRetries)) {
            result.setCompensationRetryDelay(Integer.parseInt(compensationRetryDelay));
        }
        String recoveryPolicy = sagaProperties.getProperty(RECOVERY_POLICY);
        if (!Strings.isNullOrEmpty(recoveryPolicy)) {
            result.setRecoveryPolicy(SagaRecoveryPolicy.find(recoveryPolicy));
        }
        String enabledPersistence = sagaProperties.getProperty(ENABLED_PERSISTENCE);
        if (!Strings.isNullOrEmpty(enabledPersistence)) {
            result.setEnablePersistence(Boolean.parseBoolean(enabledPersistence));
        }
        return result;
    }
}
