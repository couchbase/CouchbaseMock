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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.couchbase.mock.memcached.DataStore;
import org.couchbase.mock.memcached.MemcachedServer;

/**
 *
 * @author trond
 */
public abstract class Bucket {
    protected final DataStore datastore;
    protected final MemcachedServer servers[];
    protected final int numVBuckets;
    protected final String poolName = "default";
    protected final String name;

    public String getBucketUri() {
        return "/pools/" + poolName + "/bucketsStreaming/" + name;
    }

    public Bucket(String name, String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets) throws IOException {
        this.name = name;
        this.numVBuckets = numVBuckets;
        datastore = new DataStore(numVBuckets);
        servers = new MemcachedServer[numNodes];
        for (int ii = 0; ii < servers.length; ii++) {
            servers[ii] = new MemcachedServer(hostname, (bucketStartPort == 0 ? 0 : bucketStartPort + ii), datastore);
        }

        // Let's start distribute the vbuckets across the servers
        Random random = new Random();
        for (int ii = 0; ii < numVBuckets; ++ii) {
            int idx = random.nextInt(servers.length);
            datastore.setOwnership(ii, servers[idx]);
        }
    }

    public abstract String getJSON();

    void failSome(float percentage) {
        for (int ii = 0; ii < servers.length; ii++) {
            if (ii % percentage == 0) {
                servers[ii].shutdown();
            }
        }
    }

    void fixSome(float percentage) {

        for (int ii = 0; ii < servers.length; ii++) {
            if (ii % percentage == 0) {
                servers[ii].startup();
            }
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

}
