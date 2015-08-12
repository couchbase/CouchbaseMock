/**
 *     Copyright 2011 Membase, Inc.
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

package org.couchbase.mock.memcached;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 * @author Trond Norbye
 */
public class VBucketInfo {
    @SuppressWarnings("FieldCanBeLocal")
    private final int REPLICAS_MAX = 3;
    private MemcachedServer owner;
    private final List<MemcachedServer> replicas = new ArrayList<MemcachedServer>();
    private volatile long uuid;

    public VBucketInfo(MemcachedServer owner) {
        this.owner = owner;
    }

    public VBucketInfo() {
        this.owner = null;
        regenerateUuid();
    }

    public synchronized void setOwner(MemcachedServer server) {
        owner = server;
    }

    public synchronized void setReplicas(List<MemcachedServer> rl) {
        replicas.clear();
        if (rl.size() > REPLICAS_MAX) {
            rl = rl.subList(0, REPLICAS_MAX);
        }
        replicas.addAll(rl);
    }

    public List<MemcachedServer> getAllServers() {
        List<MemcachedServer> allServers = new ArrayList<MemcachedServer>();
        allServers.addAll(replicas);
        allServers.add(owner);
        return allServers;
    }

    public synchronized List<MemcachedServer> getReplicas() {
        return new ArrayList<MemcachedServer>(replicas);
    }

    public synchronized MemcachedServer getOwner() {
        return owner;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasAccess(MemcachedServer server) {
        if (server == owner) {
            return true;
        }
        for (MemcachedServer replica : replicas) {
            if (replica == server) {
                return true;
            }
        }
        return false;
    }

    /* Used by Bucket */
    public void regenerateUuid() {
        uuid = new Random().nextLong();
    }

    public long getUuid() {
        return uuid;
    }
}
