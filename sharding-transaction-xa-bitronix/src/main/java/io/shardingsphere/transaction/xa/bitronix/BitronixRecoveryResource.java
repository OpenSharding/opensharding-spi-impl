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

package io.shardingsphere.transaction.xa.bitronix;

import bitronix.tm.internal.XAResourceHolderState;
import bitronix.tm.resource.common.ResourceBean;
import bitronix.tm.resource.common.XAResourceHolder;
import bitronix.tm.resource.common.XAResourceProducer;
import bitronix.tm.resource.common.XAStatefulHolder;
import io.shardingsphere.transaction.xa.spi.SingleXAResource;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import javax.naming.Reference;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

/**
 * Bitronix recovery resource.
 *
 * @author zhaojun
 */
@RequiredArgsConstructor
public class BitronixRecoveryResource extends ResourceBean implements XAResourceProducer {
    
    private static final long serialVersionUID = 3352890484951804392L;
    
    private final String resourceName;
    
    private final XADataSource xaDataSource;
    
    private XAConnection xaConnection;
    
    public String getUniqueName() {
        return resourceName;
    }
    
    @SneakyThrows
    public XAResourceHolderState startRecovery() {
        xaConnection = xaDataSource.getXAConnection();
        SingleXAResourceHolder singleXAResourceHolder = new SingleXAResourceHolder(xaConnection.getXAResource(), this);
        return new XAResourceHolderState(singleXAResourceHolder, this);
    }
    
    @SneakyThrows
    public void endRecovery() {
        if (null != xaConnection) {
            xaConnection.close();
        }
    }
    
    public void setFailed(boolean failed) {
    }
    
    public XAResourceHolder findXAResourceHolder(XAResource xaResource) {
        SingleXAResource singleXAResource = (SingleXAResource) xaResource;
        return resourceName.equals(singleXAResource.getResourceName()) ? new SingleXAResourceHolder(xaResource, this) : null;
    }
    
    public void init() {
    }
    
    public void close() {
    }
    
    public XAStatefulHolder createPooledConnection(Object xaFactory, ResourceBean bean) {
        return null;
    }
    
    public Reference getReference() {
        return null;
    }
}
