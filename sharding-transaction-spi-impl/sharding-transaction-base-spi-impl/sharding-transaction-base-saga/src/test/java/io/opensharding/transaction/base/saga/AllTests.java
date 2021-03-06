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

package io.opensharding.transaction.base.saga;

import io.opensharding.transaction.base.hook.AllHookTests;
import io.opensharding.transaction.base.saga.actuator.AllActuatorTests;
import io.opensharding.transaction.base.saga.config.SagaConfigurationLoaderTest;
import io.opensharding.transaction.base.context.AllContextTests;
import io.opensharding.transaction.base.saga.persistence.AllPersistenceTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        SagaConfigurationLoaderTest.class,
        AllContextTests.class,
        AllHookTests.class,
        AllPersistenceTests.class,
        AllActuatorTests.class
})
public final class AllTests {
}
