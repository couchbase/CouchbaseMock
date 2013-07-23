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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;
import org.couchbase.mock.memcached.MemcachedServer;

/**
 * Representation of a membase bucket
 *
 * @author Trond Norbye
 */
public class CouchbaseBucket extends Bucket {

    public CouchbaseBucket(String name, String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, CouchbaseMock cluster, String password) throws IOException {
        super(name, hostname, port, numNodes, bucketStartPort, numVBuckets, cluster, password);
    }

    public CouchbaseBucket(String name, String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets) throws IOException {
        super(name, hostname, port, numNodes, bucketStartPort, numVBuckets, null);
    }

    @Override
    public String getJSON() {
        Map<String, Object> map = new HashMap<String, Object>();
        List<MemcachedServer> active = activeServers();
        map.put("name", name);
        map.put("bucketType", "membase");
        map.put("authType", "sasl");
        map.put("saslPassword", getPassword());
        map.put("proxyPort", 0);
        map.put("uri", "/pools/" + poolName + "/buckets/" + name);
        map.put("streamingUri", "/pools/" + poolName + "/bucketsStreaming/" + name);
        map.put("flushCacheUri", "/pools/" + poolName + "/buckets/" + name + "/controller/doFlush");
        List<String> nodes = new ArrayList<String>();
        for (MemcachedServer server : active) {
            nodes.add(server.toString());
        }
        map.put("nodes", nodes);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        Map<String, String> stats = new HashMap<String, String>();
        stats.put("uri", "/pools/" + poolName + "/buckets/" + name + "/stats");
        map.put("stats", stats);
        map.put("nodeLocator", "vbucket");

        Map<String, Object> vbm = new HashMap<String, Object>();
        vbm.put("hashAlgorithm", "CRC");
        vbm.put("numReplicas", 0);
        List<String> serverList = new ArrayList<String>();
        for (MemcachedServer server : active) {
            serverList.add(server.getSocketName());
        }
        vbm.put("serverList", serverList);
        ArrayList<ArrayList<Integer>> m = new ArrayList<ArrayList<Integer>>();
        for (short ii = 0; ii < numVBuckets; ++ii) {
            MemcachedServer resp = datastore.getVBucket(ii).getOwner();
            ArrayList<Integer> line = new ArrayList<Integer>();
            line.add(active.indexOf(resp));
            m.add(line);
        }
        vbm.put("vBucketMap", m);
        map.put("vBucketServerMap", vbm);
        return JSONObject.fromObject(map).toString();

    }

    @Override
    public BucketType getType() {
        return BucketType.COUCHBASE;
    }
}
