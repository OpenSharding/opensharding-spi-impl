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

package io.shardingsphere.orchestration.reg.etcd;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Util;
import io.etcd.jetcd.options.GetOption;
import lombok.SneakyThrows;
import org.apache.shardingsphere.orchestration.reg.api.RegistryCenter;
import org.apache.shardingsphere.orchestration.reg.api.RegistryCenterConfiguration;
import org.apache.shardingsphere.orchestration.reg.listener.DataChangedEventListener;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ETCD registry center.
 *
 * @author zhaojun
 */
public final class EtcdRegistryCenter implements RegistryCenter {
    
    private Client client;
    
    @Override
    public void init(final RegistryCenterConfiguration config) {
        client = Client.builder().endpoints(Util.toURIs(Splitter.on(",").trimResults().splitToList(config.getServerLists()))).build();
    }
    
    @Override
    @SneakyThrows
    public String get(final String key) {
        List<KeyValue> keyValues = client.getKVClient().get(ByteSequence.from(key, Charsets.UTF_8)).get().getKvs();
        return keyValues.isEmpty() ? null : keyValues.iterator().next().getValue().toString(Charsets.UTF_8);
    }
    
    @Override
    public String getDirectly(final String key) {
        return get(key);
    }
    
    @Override
    public boolean isExisted(final String key) {
        return null != get(key);
    }
    
    @Override
    @SneakyThrows
    public List<String> getChildrenKeys(final String key) {
        String prefix = key + "/";
        ByteSequence prefixByteSequence = ByteSequence.from(prefix, Charsets.UTF_8);
        GetOption getOption = GetOption.newBuilder().withPrefix(prefixByteSequence).withSortOrder(GetOption.SortOrder.ASCEND).build();
        List<KeyValue> keyValues = client.getKVClient().get(prefixByteSequence, getOption).get().getKvs();
        return keyValues.stream().map(e -> getSubNodeKeyName(prefix, e.getKey().toString(Charsets.UTF_8))).distinct().collect(Collectors.toList());
    }
    
    private String getSubNodeKeyName(final String prefix, final String fullPath) {
        String pathWithoutPrefix = fullPath.substring(prefix.length());
        return pathWithoutPrefix.contains("/") ? pathWithoutPrefix.substring(0, pathWithoutPrefix.indexOf("/")) : pathWithoutPrefix;
    }
    
    @Override
    public void persist(final String key, final String value) {
    
    }
    
    @Override
    public void update(final String key, final String value) {
    
    }
    
    @Override
    public void persistEphemeral(final String key, final String value) {
    
    }
    
    @Override
    public void watch(final String key, final DataChangedEventListener dataChangedEventListener) {
    
    }
    
    @Override
    public void close() {
    
    }
}
