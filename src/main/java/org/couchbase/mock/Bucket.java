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

import org.couchbase.mock.http.BucketAdminServer;
import org.couchbase.mock.http.capi.CAPIServer;
import org.couchbase.mock.memcached.*;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract class for all bucket types in Couchbase.
 * @see {@link org.couchbase.mock.CouchbaseBucket}
 * @see {@link org.couchbase.mock.MemcachedBucket}
 *
 * A bucket is instantiated via {@link org.couchbase.mock.CouchbaseMock#createBucket(BucketConfiguration)}. The number
 * of servers a bucket has is limited to the amount provided in the {@link org.couchbase.mock.BucketConfiguration#numNodes}
 * field.
 *
 * Nodes can be removed and then re-added, but currently new nodes cannot be added once the bucket has been instantiated.
 */
public abstract class Bucket {
    private CAPIServer capiServer = null;
    private BucketAdminServer adminServer = null;

    public enum BucketType {
        MEMCACHED,
        COUCHBASE
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

    /**
     * Returns the vBucket map for the given bucket. This is only relevant for {@link org.couchbase.mock.CouchbaseBucket}
     * @return An array of vBucket map structures
     */
    public VBucketInfo[] getVBucketInfo() {
        return vbInfo;
    }

    /**
     * Get the list of servers allocated for this bucket. This returns both active and inactive servers
     * @return an array of servers for this bucket.
     */
    public MemcachedServer[] getServers() {
        return servers;
    }

    private Iterator<Item> getMasterItemsIterator(final Storage.StorageType type) {
        return new Iterator<Item>() {
            private int curIndex = -1;

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

    /**
     * Returns an iterable over all the items in this bucket
     * @param type The storage location to fetch from
     * @return An iterable which will return all items in the bucket.
     *
     * Note this currently makes a copy of the items list, making it thread safe. It also means
     * that this will potentially return stale data
     */
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

    /**
     * Create a bucket.
     * @param mock The cluster this bucket is a member of
     * @param config The configuration for the bucket
     * @return The newly instantiated subclass
     * @throws IOException
     */
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

    /**
     * Gets the type of the bucket
     * @return The type of the bucket
     */
    public abstract BucketType getType();

    // Used internally by CouchbaseMock
    void setCAPIServer(CAPIServer server) {
        this.capiServer = server;
    }

    /**
     * Get the {@link org.couchbase.mock.http.capi.CAPIServer} object used for managing views.
     * @return The view manager
     */
    public CAPIServer getCAPIServer() {
        return capiServer;
    }

    void setAdminServer(BucketAdminServer adminServer) {
        this.adminServer = adminServer;
    }

    /**
     * Get the object used for handling configuration changes
     * @return The configuration manager
     */
    public BucketAdminServer getAdminServer() {
        return adminServer;
    }

    /**
     * Gets a map of the current bucket configuration which can be JSON-serialized
     * as a valid "Cluster configuration". This method is useful for other configuration handlers
     * which wish to embed the current bucket's configuration into a larger structure.
     *
     * The information returned is equivalent to that returned via the
     * {@code /pools/default/buckets/$bucket} endpoint in a Couchbase cluster.
     *
     * @return A map representing the cluster configuration
     *
     * Note that to avoid race conditions, invoke {@link #configReadLock()}
     * before calling this method, and {@link #configReadUnlock()} after calling
     * this method.
     */
    public abstract Map<String,Object> getConfigMap();

    /**
     * Returns configuration information common to both Couchbase and Memcached buckets
     * @return The configuration object to be injected
     */
    protected Map<String,Object> getCommonConfig() {
        Map<String,Object> mm = new HashMap<String, Object>();
        mm.put("replicaNumber", numReplicas);
        Map<String,Object> ramQuota = new HashMap<String, Object>();
        ramQuota.put("rawRAM", 1024 * 1024 * 100);
        ramQuota.put("ram", 1024 * 1024 * 100);
        mm.put("quota", ramQuota);
        return mm;
    }

    /**
     * Convenience method to get the JSON-encoded version of the configuration map.
     * @return An encoded JSON String
     * @see {@link #getConfigMap()}
     */
    public final String getJSON() {
        return JsonUtils.encode(getConfigMap());
    }

    /**
     * Lock the current configuration for reading. As long as this lock is held, any
     * configuration changes to the bucket (such as failover, removing a node, rebalances,
     * etc.) will be blocked. Be sure to call {@link #configReadUnlock()} once the lock
     * is no longer required.
     *
     * This method is most useful to ensure that the bucket state remains the same while
     * reading configuration-related properties.
     */
    public void configReadLock() {
        configurationRwLock.readLock().lock();
    }

    /**
     * Unlock the configuration read lock. This is the exit bracket for {@link #configReadLock()}
     */
    public void configReadUnlock() {
        configurationRwLock.readLock().unlock();
    }

    /**
     * Convenience method to store an item in a bucket
     * @param key The key of the item
     * @param value The value of the item
     * @return The status of the operation
     */
    public abstract ErrorCode storeItem(String key, byte[] value);

    /**
     * Fail over one of the bucket's nodes
     * @param index The index of the node to fail over. This index
     *              is the index of the node within the {@link #servers}
     *              (or {@link #getServers()} array; not the logical index
     *              within the vBucket map!
     *
     * Note this will also automatically rebalance the cluster
     */
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

    /**
     * Re-Add a previously failed-over node
     * @param index The index to restore. This should be an index into the
     *              {@link #servers} array.
     */
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

    void start() {
        for (int ii = 0; ii < servers.length; ii++) {
            servers[ii].setName(String.format("%s:MCD[%d]", name, ii));
            servers[ii].setDaemon(true);
            servers[ii].start();
        }
    }

    void stop() {
        for (MemcachedServer t : servers) {
            t.interrupt();
            do {
                try {
                    t.join();
                    t = null;
                } catch (InterruptedException ex) {
                    Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                    t.interrupt();
                }
            } while (t != null);
        }
    }

    /**
     * Gets the list of <b>active</b> nodes within the bucket. An active node is one that
     * is not failed over
     * @return The list of active nodes in the cluster.
     */
    public List<MemcachedServer> activeServers() {
        ArrayList<MemcachedServer> active = new ArrayList<MemcachedServer>(servers.length);
        for (MemcachedServer server : servers) {
            if (server.isActive()) {
                active.add(server);
            }
        }
        return active;
    }

    /**
     * Issues a rebalance within the bucket. vBuckets which are mapped to failed-over
     * nodes are relocated with their first replica being promoted to active.
     */
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

    public void regenCoords() {
        for (VBucketInfo cur : vbInfo) {
            cur.regenerateUuid();
        }
        for (MemcachedServer s : servers) {
            s.getStorage().updateCoordinateInfo(vbInfo);
        }
    }

    /**
     * Get the password for this bucket.
     * @return The password
     */
    public String getPassword() {
        return password;
    }

    /** Get the name of the bucket */
    public String getName() {
        return name;
    }

    /** Gets the UUID for the bucket. This is only used to generate the stats response */
    public String getUUID() {
        return uuid.toString();
    }

    /** Gets the parent {@link org.couchbase.mock.CouchbaseMock} object */
    public CouchbaseMock getCluster() {
        return cluster;
    }
}
