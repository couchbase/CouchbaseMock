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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.MemcachedServer;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Representation of a CacheBucket (aka memcached)
 *
 * @author Trond Norbye
 */
public class MemcachedBucket extends Bucket {
    public MemcachedBucket(CouchbaseMock cluster, BucketConfiguration config)
            throws IOException
    {
        super(cluster, config);
    }

    @Override
    public Map<String,Object> getConfigMap() {
        Map<String, Object> map = getCommonConfig();

        map.put("name", name);
        map.put("authType", "sasl");
        map.put("bucketType", "memcached");

        map.put("flushCacheUri", "/pools/" + poolName + "/buckets/" + name + "/controller/doFlush");
        map.put("nodeLocator", "ketama");
        map.put("proxyPort", 0);
        map.put("replicaNumber", 0);
        map.put("saslPassword", getPassword());
        map.put("streamingUri", "/pools/" + poolName + "/bucketsStreaming/" + name);
        map.put("uri", "/pools/" + poolName + "/buckets/" + name);

        List<Object> nodes = new ArrayList<Object>();
        for (MemcachedServer server : activeServers()) {
            nodes.add(server.toNodeConfigInfo());
        }
        map.put("nodes", nodes);
        return map;
    }

    @Override
    public BucketType getType() {
        return BucketType.MEMCACHED;
    }

    @Override
    public ErrorCode storeItem(String key, byte[] value) {
        throw new UnsupportedOperationException("Storing items not yet supported for memcached buckets!");
    }
}
