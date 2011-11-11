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
import org.couchbase.mock.memcached.MemcachedServer;
import org.couchbase.mock.util.JSON;

/**
 *
 * @author trond
 */
public class MembaseBucket extends Bucket {

    public MembaseBucket(String name, String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets) throws IOException {
        super(name, hostname, port, numNodes, bucketStartPort, numVBuckets);
    }

    @Override
    public String getJSON() {

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{");
        JSON.addElement(pw, "name", name, true);
        JSON.addElement(pw, "bucketType", "membase", true);
        JSON.addElement(pw, "authType", "sasl", true);
        JSON.addElement(pw, "saslPassword", "", true);
        JSON.addElement(pw, "proxyPort", 0, true);
        JSON.addElement(pw, "uri", "/pools/" + poolName + "/buckets/" + name, true);
        JSON.addElement(pw, "streamingUri", "/pools/" + poolName + "/bucketsStreaming/" + name, true);
        JSON.addElement(pw, "flushCacheUri", "/pools/" + poolName + "/buckets/" + name + "/controller/doFlush", true);
        pw.print("\"nodes\":[");
        for (int ii = 0; ii < servers.length; ++ii) {
            pw.print(servers[ii].toString());
            if (ii != servers.length - 1) {
                pw.print(",");
            }
        }
        pw.print("],");
        pw.print("\"stats\":{\"uri\":\"/pools/" + poolName + "/buckets/default/stats\"},");
        JSON.addElement(pw, "nodeLocator", "vbucket", true);

        pw.print("\"vBucketServerMap\":{");
        JSON.addElement(pw, "hashAlgorithm", "CRC", true);
        JSON.addElement(pw, "numReplicas", 0, true);

        pw.print("\"serverList\":[");
        for (int ii = 0; ii < servers.length; ++ii) {
            pw.print('"');
            pw.print(servers[ii].getSocketName());
            pw.print('"');
            if (ii != servers.length - 1) {
                pw.print(',');
            }
        }

        pw.print("],\"vBucketMap\":[");

        for (short ii = 0; ii < numVBuckets; ++ii) {
            MemcachedServer resp = datastore.getVBucket(ii).getOwner();
            for (int jj = 0; jj < servers.length; ++jj) {
                if (resp == servers[jj]) {
                    pw.print("[" + jj + "]");
                    break;
                }
            }
            if (ii != numVBuckets - 1) {
                pw.print(",");
            }
        }

        pw.print("]}}");
        pw.flush();
        return sw.toString();

    }
}
