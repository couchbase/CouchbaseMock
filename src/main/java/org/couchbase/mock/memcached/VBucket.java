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

import java.security.AccessControlException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.couchbase.mock.Bucket.BucketType;

/**
 * @author Trond Norbye
 */
public class VBucket {

    private MemcachedServer owner;
    private Map<String, Item> map;

    public VBucket(MemcachedServer owner) {
        this.owner = owner;
        map = new ConcurrentHashMap<String, Item>();

    }

    public synchronized void setOwner(MemcachedServer server) {
        owner = server;
    }

    public synchronized Map<String, Item> getMap(MemcachedServer server) {
        if (server.getType() == BucketType.COUCHBASE && server != owner) {
            throw new AccessControlException("Not my VBucket");
        }
        return map;
    }

    public synchronized MemcachedServer getOwner() {
        return owner;
    }

    void flush(MemcachedServer server) {
        if (owner == server) {
            map.clear();
        }
    }
}
