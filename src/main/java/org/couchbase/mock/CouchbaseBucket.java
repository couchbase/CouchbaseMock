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
import java.util.zip.CRC32;
import org.couchbase.mock.memcached.MemcachedServer;

/**
 * Representation of a membase bucket
 *
 * @author Trond Norbye
 */
public class CouchbaseBucket extends Bucket {
    public CouchbaseBucket(CouchbaseMock cluster, BucketConfiguration config)
    throws IOException
    {
        super(cluster, config);
    }

    @Override
    public short getVbIndexForKey(String key) {
        CRC32 crc32 = new CRC32();
        crc32.update(key.getBytes());
        long digest = ( crc32.getValue() >> 16 ) & 0x7fff;
        long vbKey = digest & ( vbInfo.length - 1 );
        return (short) vbKey;
    }

    @Override
    public Map<String,Object> getConfigMap() {
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
        List<Map> nodes = new ArrayList<Map>();
        for (MemcachedServer server : active) {
            Map<String,Object> nodeInfo = server.toNodeConfigInfo();
            if (cluster != null) {
                // Add 'couchApiBase'
                String capiBase = String.format("http://%s:%d/%s", cluster.getHttpHost(), cluster.getHttpPort(), name);
                nodeInfo.put("couchApiBase", capiBase);
            }
            nodes.add(nodeInfo);
        }
        map.put("nodes", nodes);


        Map<String, String> stats = new HashMap<String, String>();
        stats.put("uri", "/pools/" + poolName + "/buckets/" + name + "/stats");
        map.put("stats", stats);
        map.put("nodeLocator", "vbucket");

        Map<String, Object> vbm = new HashMap<String, Object>();
        vbm.put("hashAlgorithm", "CRC");
        vbm.put("numReplicas", numReplicas);
        List<String> serverList = new ArrayList<String>();
        for (MemcachedServer server : active) {
            serverList.add(server.getSocketName());
        }
        vbm.put("serverList", serverList);
        ArrayList<ArrayList<Integer>> m = new ArrayList<ArrayList<Integer>>();
        for (short ii = 0; ii < numVBuckets; ++ii) {
            MemcachedServer master = vbInfo[ii].getOwner();
            List<MemcachedServer> replicas = vbInfo[ii].getReplicas();
            ArrayList<Integer> line = new ArrayList<Integer>();
            line.add(active.indexOf(master));
            for (MemcachedServer replica : replicas) {
                line.add(active.indexOf(replica));
            }

            // If numReplicas is greater than list.size() - 1
            // (because we also have the master) then fill it with
            // zeros until we have the desired count
            while (line.size()-1 < numReplicas) {
                line.add(-1);
            }

            m.add(line);
        }
        vbm.put("vBucketMap", m);
        map.put("vBucketServerMap", vbm);
        return map;
    }

    @Override
    public BucketType getType() {
        return BucketType.COUCHBASE;
    }
}
