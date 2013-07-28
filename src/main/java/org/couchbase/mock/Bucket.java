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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.couchbase.mock.memcached.DataStore;
import org.couchbase.mock.memcached.MemcachedServer;

/**
 * Representation of the bucket in the membase concept.
 *
 * @author Trond Norbye
 */
public abstract class Bucket {

    public DataStore getDataStore() {
        return datastore;
    }

    public enum BucketType {
        MEMCACHED, COUCHBASE
    }

    final DataStore datastore;
    private final MemcachedServer[] servers;
    final int numVBuckets;
    final String poolName = "default";
    final String name;
    private final UUID uuid;
    private final CouchbaseMock cluster;
    private final String password;
    private final ReentrantReadWriteLock configurationRwLock;

    public String getBucketUri() {
        return "/pools/" + poolName + "/bucketsStreaming/" + name;
    }

    Bucket(String name, String hostname, int numNodes, int bucketStartPort, int numVBuckets, CouchbaseMock cluster) throws IOException {
        this(name, hostname, numNodes, bucketStartPort, numVBuckets, cluster, "");
    }

    public MemcachedServer[] getServers() {
        return servers;
    }

    Bucket(String name, String hostname, int numNodes, int bucketStartPort, int numVBuckets, CouchbaseMock cluster, String password) throws IOException {
        this.cluster = cluster;
        this.name = name;
        this.numVBuckets = numVBuckets;
        this.password = password;
        datastore = new DataStore(numVBuckets);
        servers = new MemcachedServer[numNodes];
        uuid = UUID.randomUUID();
        this.configurationRwLock = new ReentrantReadWriteLock();

        if (this.getClass() != MemcachedBucket.class && this.getClass() != CouchbaseBucket.class) {
            throw new FileNotFoundException("I don't know about this type...");
        }
        for (int ii = 0; ii < servers.length; ii++) {
            servers[ii] = new MemcachedServer(this, hostname, (bucketStartPort == 0 ? 0 : bucketStartPort + ii), datastore);
        }

        rebalance();
    }

    public static Bucket create(BucketType type, String name, String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, CouchbaseMock cluster, String password) throws IOException {
        switch (type) {
            case MEMCACHED:
                return new MemcachedBucket(name, hostname, port, numNodes, bucketStartPort, numVBuckets, cluster, password);
            case COUCHBASE:
                return new CouchbaseBucket(name, hostname, port, numNodes, bucketStartPort, numVBuckets, cluster, password);
            default:
                throw new FileNotFoundException("I don't know about this type...");
        }
    }

    public abstract BucketType getType();

    public abstract String getJSON();

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
            Random random = new Random();
            List<MemcachedServer> nodes = activeServers();
            for (int ii = 0; ii < numVBuckets; ++ii) {
                int idx = random.nextInt(nodes.size());
                datastore.setOwnership(ii, nodes.get(idx));
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
}
