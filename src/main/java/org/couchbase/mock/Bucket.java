/**
 *     Copyright 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.couchbase.mock;

import org.couchbase.mock.memcached.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Representation of the bucket in the membase concept.
 *
 * @author Trond Norbye
 */
public abstract class Bucket {
    public enum BucketType {
        MEMCACHED, COUCHBASE
    }

    protected final VBucketInfo vbInfo[];
    protected final MemcachedServer servers[];
    protected final int numVBuckets;
    protected final int numReplicas;
    protected final String poolName = "default";
    protected final String name;
    protected final CouchbaseMock cluster;
    protected final String password;
    protected final ReentrantReadWriteLock configurationRwLock;
    private final UUID uuid;

    public String getBucketUri() {
        return "/pools/" + poolName + "/bucketsStreaming/" + name;
    }

    public VBucketInfo[] getVBucketInfo() {
        return vbInfo;
    }

    public MemcachedServer[] getServers() {
        return servers;
    }

    private Iterator<Item> getMasterItemsIterator(final Storage.StorageType type) {
        return new Iterator<Item>() {
            private int curIndex = -1;
            private int counter = 0;

            private Iterator<Item> getNextIterator() {
                if (++curIndex == servers.length) {
                    return null;
                }
                MemcachedServer s = servers[curIndex];
                return s.getStorage().getMasterStore(type).iterator();
            }

            private Iterator<Item> curIterator = getNextIterator();

            @Override
            public boolean hasNext() {
                while (!curIterator.hasNext()) {
                    curIterator = getNextIterator();
                    if (curIterator == null) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Item next() {
                return curIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    public Iterable<Item> getMasterItems(final Storage.StorageType type) {
        return new Iterable<Item>() {
            @Override
            public Iterator<Item> iterator() {
                return getMasterItemsIterator(type);
            }
        };
    }

    /**
     * Get the server index for a given key
     * @param key The key to look up
     * @return an index which can be used in the servers array (via getServers)
     */
    public short getVbIndexForKey(String key) {
        return -1;
    }

    public Bucket(CouchbaseMock cluster, BucketConfiguration config) throws IOException {
        if (config.numVBuckets < 0) {
            throw new IllegalArgumentException("Vbucket count must be > 0");
        }

        if ( (config.numVBuckets & (config.numVBuckets-1)) != 0 ) {
            throw new IllegalArgumentException(
                    "vBucket count must be a power of 2");
        }

        this.cluster = cluster;
        name = config.name;
        numVBuckets = config.numVBuckets;
        numReplicas = config.numReplicas;
        password = config.password;

        vbInfo = new VBucketInfo[numVBuckets];
        servers = new MemcachedServer[config.numNodes];
        uuid = UUID.randomUUID();

        this.configurationRwLock = new ReentrantReadWriteLock();

        for (int ii = 0; ii < vbInfo.length; ii++) {
            vbInfo[ii] = new VBucketInfo();
        }

        if (this.getClass() != MemcachedBucket.class && this.getClass() != CouchbaseBucket.class) {
            throw new FileNotFoundException("I don't know about this type...");
        }
        for (int ii = 0; ii < servers.length; ii++) {
            servers[ii] = new MemcachedServer(this,
                    config.hostname,
                    (config.bucketStartPort == 0 ? 0 : config.bucketStartPort + ii),
                    vbInfo);
        }

        rebalance();
    }

    public static Bucket create(CouchbaseMock mock, BucketConfiguration config) throws IOException {
          switch (config.type) {
                case MEMCACHED:
                    return new MemcachedBucket(mock, config);
                case COUCHBASE:
                    return new CouchbaseBucket(mock, config);
                default:
                    throw new FileNotFoundException("I don't know about this type...");
            }
    }

    public abstract BucketType getType();
    public abstract Map<String,Object> getConfigMap();
    public final String getJSON() {
        return JsonUtils.encode(getConfigMap());
    }

    public void configReadLock() {
        configurationRwLock.readLock().lock();
    }

    public void configReadUnlock() {
        configurationRwLock.readLock().unlock();
    }

    void failSome(float percentage) {
        configurationRwLock.writeLock().lock();
        try {
            for (int ii = 0; ii < servers.length; ii++) {
                if (ii % percentage == 0) {
                    servers[ii].shutdown();
                }
            }
        } finally {
            configurationRwLock.writeLock().unlock();
        }
    }

    void fixSome(float percentage) {
        configurationRwLock.writeLock().lock();
        try {
            for (int ii = 0; ii < servers.length; ii++) {
                if (ii % percentage == 0) {
                    servers[ii].startup();
                }
            }
        } finally {
            configurationRwLock.writeLock().unlock();
        }
    }



    public void failover(int index) {
        configurationRwLock.writeLock().lock();
        try {
            if (index >= 0 && index < servers.length) {
                servers[index].shutdown();
            }
            rebalance();
        } finally {
            configurationRwLock.writeLock().unlock();
        }
    }

    public void respawn(int index) {
        configurationRwLock.writeLock().lock();
        try {
            if (index >= 0 && index < servers.length) {
                servers[index].startup();
            }
            rebalance();
        } finally {
            configurationRwLock.writeLock().unlock();
        }
    }

    void start(List<Thread> threads) {
        for (int ii = 0; ii < servers.length; ii++) {
            Thread t = new Thread(servers[ii], "mock memcached " + ii);
            t.setDaemon(true);
            t.start();
            threads.add(t);
        }
    }

    public List<MemcachedServer> activeServers() {
        ArrayList<MemcachedServer> active = new ArrayList<MemcachedServer>(servers.length);
        for (MemcachedServer server : servers) {
            if (server.isActive()) {
                active.add(server);
            }
        }
        return active;
    }

    final void rebalance() {
        // Let's start distribute the vbuckets across the servers
        configurationRwLock.writeLock().lock();
        try {
            List<MemcachedServer> nodes = activeServers();
            for (int ii = 0; ii < numVBuckets; ++ii) {
                Collections.shuffle(nodes);
                vbInfo[ii].setOwner(nodes.get(0));
                if (nodes.size() < 2) {
                    continue;
                }
                List<MemcachedServer> replicas = nodes.subList(1, nodes.size());
                if (replicas.size() > numReplicas) {
                    replicas = replicas.subList(0, numReplicas);
                }
                vbInfo[ii].setReplicas(replicas);
            }
        } finally {
            configurationRwLock.writeLock().unlock();
        }
    }

    public String getPassword() {
        return password;
    }

    public String getName() {
        return name;
    }

    public String getUUID() {
        return uuid.toString();
    }

    public CouchbaseMock getCluster() {
        return cluster;
    }
}
