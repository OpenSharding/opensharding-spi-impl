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

package io.opensharding.orchestration.reg.zookeeper.natived;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import io.opensharding.orchestration.reg.zookeeper.natived.client.action.IZookeeperClient;
import io.opensharding.orchestration.reg.zookeeper.natived.client.cache.PathTree;
import io.opensharding.orchestration.reg.zookeeper.natived.client.retry.DelayRetryPolicy;
import io.opensharding.orchestration.reg.zookeeper.natived.client.utility.ZookeeperConstants;
import io.opensharding.orchestration.reg.zookeeper.natived.client.zookeeper.ClientFactory;
import io.opensharding.orchestration.reg.zookeeper.natived.client.zookeeper.section.StrategyType;
import io.opensharding.orchestration.reg.zookeeper.natived.client.zookeeper.section.ZookeeperEventListener;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.orchestration.reg.api.RegistryCenter;
import org.apache.shardingsphere.orchestration.reg.api.RegistryCenterConfiguration;
import org.apache.shardingsphere.orchestration.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.orchestration.reg.listener.DataChangedEventListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Registry center for native zookeeper.
 * 
 * @author lidongbo
 */
public final class NativeZookeeperRegistryCenter implements RegistryCenter {
    
    private final Map<String, PathTree> caches = new HashMap<>();
    
    private IZookeeperClient client;
    
    @Getter
    @Setter
    private Properties properties;
    
    @Override
    public void init(final RegistryCenterConfiguration config) {
        client = initClient(buildClientFactory(config), config);
    }
    
    private ClientFactory buildClientFactory(final RegistryCenterConfiguration config) {
        ClientFactory result = new ClientFactory();
        result.setClientNamespace(config.getNamespace()).newClient(config.getServerLists(), config.getTimeToLiveSeconds() * 1000)
                .setRetryPolicy(new DelayRetryPolicy(config.getRetryIntervalMilliseconds(), config.getMaxRetries(), config.getRetryIntervalMilliseconds()));
        if (!Strings.isNullOrEmpty(config.getDigest())) {
            result.authorization("digest", config.getDigest().getBytes(Charsets.UTF_8), ZooDefs.Ids.CREATOR_ALL_ACL);
        }
        return result;
    }
    
    private IZookeeperClient initClient(final ClientFactory clientFactory, final RegistryCenterConfiguration config) {
        IZookeeperClient result = null;
        try {
            // TODO There is a bug when the start time is very short, and I haven't found the reason yet
            // result = clientFactory.start(config.getRetryIntervalMilliseconds() * config.getMaxRetries(), TimeUnit.MILLISECONDS);
            result = clientFactory.start();
            if (!result.blockUntilConnected(config.getRetryIntervalMilliseconds() * config.getMaxRetries(), TimeUnit.MILLISECONDS)) {
                result.close();
                throw new KeeperException.OperationTimeoutException();
            }
            result.useExecStrategy(StrategyType.SYNC_RETRY);
        } catch (final KeeperException.OperationTimeoutException | IOException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
        }
        return result;
    }
    
    @Override
    public String get(final String key) {
        Optional<PathTree> cache = findTreeCache(key);
        if (!cache.isPresent()) {
            return getDirectly(key);
        }
        byte[] resultInCache = cache.get().getValue(key);
        if (null != resultInCache) {
            return new String(resultInCache, Charsets.UTF_8);
        }
        return getDirectly(key);
    }
    
    private Optional<PathTree> findTreeCache(final String key) {
        for (Entry<String, PathTree> entry : caches.entrySet()) {
            if (key.startsWith(entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.absent();
    }
    
    @Override
    public String getDirectly(final String key) {
        try {
            return new String(client.getData(key), Charsets.UTF_8);
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
            return null;
        }
    }
    
    @Override
    public boolean isExisted(final String key) {
        try {
            return client.checkExists(key);
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
            return false;
        }
    }
    
    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            final List<String> result = client.getChildren(key);
            Collections.sort(result, new Comparator<String>() {
                
                @Override
                public int compare(final String o1, final String o2) {
                    return o2.compareTo(o1);
                }
            });
            return result;
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
            return Collections.emptyList();
        }
    }
    
    @Override
    public void persist(final String key, final String value) {
        try {
            if (!isExisted(key)) {
                client.createAllNeedPath(key, value, CreateMode.PERSISTENT);
            } else {
                update(key, value);
            }
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void update(final String key, final String value) {
        try {
            client.transaction().check(key, ZookeeperConstants.VERSION).setData(key, value.getBytes(ZookeeperConstants.UTF_8), ZookeeperConstants.VERSION).commit();
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void persistEphemeral(final String key, final String value) {
        try {
            if (isExisted(key)) {
                client.deleteAllChildren(key);
            }
            client.createAllNeedPath(key, value, CreateMode.EPHEMERAL);
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void watch(final String key, final DataChangedEventListener eventListener) {
        String path = key + "/";
        if (!caches.containsKey(path)) {
            addCacheData(key);
        }
        PathTree cache = caches.get(path);
        cache.watch(new ZookeeperEventListener() {
            
            @Override
            public void process(final WatchedEvent event) {
                if (!Strings.isNullOrEmpty(event.getPath())) {
                    eventListener.onChange(new DataChangedEvent(event.getPath(), getWithoutCache(event.getPath()), getEventChangedType(event)));
                }
            }
        });
    }
    
    private DataChangedEvent.ChangedType getEventChangedType(final WatchedEvent event) {
        switch (event.getType()) {
            case NodeDataChanged:
            case NodeChildrenChanged:
                return DataChangedEvent.ChangedType.UPDATED;
            case NodeDeleted:
                return DataChangedEvent.ChangedType.DELETED;
            default:
                return DataChangedEvent.ChangedType.IGNORED;
        }
    }
    
    private synchronized String getWithoutCache(final String key) {
        try {
            client.useExecStrategy(StrategyType.USUAL);
            byte[] data = client.getData(key);
            client.useExecStrategy(StrategyType.SYNC_RETRY);
            return null == data ? null : new String(data, Charsets.UTF_8);
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
            return null;
        }
    }
    
    private void addCacheData(final String cachePath) {
        PathTree cache = new PathTree(cachePath, client);
        try {
            cache.load();
            cache.watch();
        } catch (final KeeperException | InterruptedException ex) {
            NativeZookeeperExceptionHandler.handleException(ex);
        }
        caches.put(cachePath + "/", cache);
    }
    
    @Override
    public void close() {
        for (Entry<String, PathTree> each : caches.entrySet()) {
            each.getValue().close();
        }
        client.close();
    }
    
    @Override
    public void initLock(String key) {
    
    }
    
    @Override
    public boolean tryLock() {
        return false;
    }
    
    @Override
    public void tryRelease() {
    
    }
    
    @Override
    public String getType() {
        return "native-zookeeper";
    }
}
