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
package org.couchbase.mock;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.List;
import java.util.logging.Logger;
import java.util.Random;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.net.Socket;
import org.couchbase.mock.http.HttpReasonCode;
import org.couchbase.mock.http.HttpRequest;
import org.couchbase.mock.http.HttpRequestHandler;
import org.couchbase.mock.http.HttpRequestImpl;
import org.couchbase.mock.http.HttpServer;
import org.couchbase.mock.memcached.DataStore;
import org.couchbase.mock.memcached.MemcachedServer;
import org.couchbase.mock.util.Base64;
import org.couchbase.mock.util.JSON;
import org.couchbase.mock.util.Getopt;
import org.couchbase.mock.util.Getopt.CommandLineOption;
import org.couchbase.mock.util.Getopt.Entry;

/**
 * This is a super-scaled down version of something that might look like
 * membase ;-) It provides the REST interface to our bucket lists, so that
 * you may use it to retrieve a list of servers and where their vbuckets
 * are...
 *
 * @author Trond Norbye
 */
public class CouchbaseMock implements HttpRequestHandler, Runnable {

    private void setupHarakiriMonitor(String host) {
        int idx = host.indexOf(':');
        String h = host.substring(0, idx);
        int p = Integer.parseInt(host.substring(idx + 1));
        try {
            HarakiriMonitor m = new HarakiriMonitor(h, p, httpServer.getPort(), true);
            Thread t = new Thread(m, "HarakiriMonitor");
            t.start();
        } catch (Throwable t) {
            System.err.println("Failed to set up harakiri monitor: " + t.getMessage());
            System.exit(1);
        }
    }

    protected static class HarakiriMonitor implements Runnable {
        private final boolean terminate;
        private final InputStream stream;

        public HarakiriMonitor(String host, int port, int httpPort, boolean terminate) throws IOException {
            this.terminate = terminate;
            Socket s = new Socket(host, port);
            stream = s.getInputStream();
            String http = "" + httpPort + '\0';
            s.getOutputStream().write(http.getBytes());
            s.getOutputStream().flush();
        }

        @Override
        public void run() {
            boolean closed = false;
            while (!closed) {
                try {
                    if (stream.read() == -1) {
                        closed = true;
                    }
                } catch (IOException e) {
                    // not exactly true, but who cares..
                    closed = true;
                }
            }

            if (terminate) {
                System.exit(1);
            }
        }
    }

    private final DataStore datastore;
    private final MemcachedServer servers[];
    private final int numVBuckets;
    private final HttpServer httpServer;
    private final BucketType defaultBucketType;

    public CouchbaseMock(int port, int numNodes, int bucketStartPort, int numVBuckets, CouchbaseMock.BucketType type) throws IOException {
        this.numVBuckets = numVBuckets;
        datastore = new DataStore(numVBuckets);
        this.defaultBucketType = type;
        servers = new MemcachedServer[numNodes];
        for (int ii = 0; ii < servers.length; ii++) {
            servers[ii] = new MemcachedServer((bucketStartPort == 0 ? 0 : bucketStartPort + ii), datastore);
        }

        // Let's start distribute the vbuckets across the servers
        Random random = new Random();
        for (int ii = 0; ii < numVBuckets; ++ii) {
            int idx = random.nextInt(servers.length);
            datastore.setOwnership(ii, servers[idx]);
        }

        httpServer = new HttpServer(port);
    }

    public CouchbaseMock(int port, int numNodes, int numVBuckets, CouchbaseMock.BucketType type) throws IOException {
        this(port, numNodes, 0, numVBuckets, type);
    }

    public CouchbaseMock(int port, int numNodes, int numVBuckets) throws IOException {
	this(port, numNodes, numVBuckets, BucketType.BASE);
    }

    private byte[] getBucketJSON() {
        switch (this.defaultBucketType) {
            case CACHE: return getCacheBucketDefaultJSON();
            default: return getMembaseBucketDefaultJSON();
        }
    }

    private byte[] getMembaseBucketDefaultJSON() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{");
        JSON.addElement(pw, "name", "default", true);
        JSON.addElement(pw, "bucketType", "membase", true);
        JSON.addElement(pw, "authType", "sasl", true);
        JSON.addElement(pw, "saslPassword", "", true);
        JSON.addElement(pw, "proxyPort", 0, true);
        JSON.addElement(pw, "uri", "/pools/default/buckets/default", true);
        JSON.addElement(pw, "streamingUri", "/pools/default/bucketsStreaming/default", true);
        JSON.addElement(pw, "flushCacheUri", "/pools/default/buckets/default/controller/doFlush", true);
        pw.print("\"nodes\":[");
        for (int ii = 0; ii < servers.length; ++ii) {
            pw.print(servers[ii].toString());
            if (ii != servers.length - 1) {
                pw.print(",");
            }
        }
        pw.print("],");
        pw.print("\"stats\":{\"uri\":\"/pools/default/buckets/default/stats\"},");
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
        return sw.toString().getBytes();
    }

        private byte[] getCacheBucketDefaultJSON() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{");
        JSON.addElement(pw, "name", "cache", true);
        JSON.addElement(pw, "authType", "sasl", true);
        //    "basicStats": {
        //        "diskUsed": 0,
        //        "hitRatio": 0,
        //        "itemCount": 10001,
        //        "memUsed": 27007687,
        //        "opsPerSec": 0,
        //        "quotaPercentUsed": 10.061147436499596
        //    },
        JSON.addElement(pw, "bucketType", "memcached", true);
        JSON.addElement(pw, "flushCacheUri", "/pools/default/buckets/default/controller/doFlush", true);
        JSON.addElement(pw, "name", "default", true);
        JSON.addElement(pw, "nodeLocator", "ketama", true);
        // NOTE: nodes are done with the code below, but this listing is kept for future reference
        //    "nodes": [
        //        {
        //            "clusterCompatibility": 1,
        //            "clusterMembership": "active",
        //            "hostname": "127.0.0.1:8091",
        //            "mcdMemoryAllocated": 2985,
        //            "mcdMemoryReserved": 2985,
        //            "memoryFree": 285800000,
        //            "memoryTotal": 3913584000.0,
        //            "os": "i386-apple-darwin9.8.0",
        //            "ports": {
        //                "direct": 11210,
        //                "proxy": 11211
        //            },
        //            "replication": 1.0,
        //            "status": "unhealthy",
        //            "uptime": "4204",
        //            "version": "1.6.5"
        //        }
        //    ],
        pw.print("\"nodes\":[");
        for (int ii = 0; ii < servers.length; ++ii) {
            pw.print(servers[ii].toString());
            if (ii != servers.length - 1) {
                pw.print(",");
            }
        }
        pw.print("],");
        JSON.addElement(pw, "proxyPort", 0, true);
        //    "quota": {
        //        "ram": 268435456,
        //        "rawRAM": 268435456
        //    },
        JSON.addElement(pw, "replicaNumber", 0, true);
        JSON.addElement(pw, "saslPassword", "", true);
        //    "stats": {
        //        "uri": "/pools/default/buckets/default/stats"
        //    },
        JSON.addElement(pw, "streamingUri", "/pools/default/bucketsStreaming/default", true);
        JSON.addElement(pw, "uri", "/pools/default/buckets/default", false);

        pw.print("}");
        pw.flush();
        return sw.toString().getBytes();
    }

    private byte[] getPoolsJSON() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{\"pools\":[{\"name\":\"default\",\"uri\":\"/pools/default\","
                + "\"streamingUri\":\"/poolsStreaming/default\"}],\"isAdminCreds\":true,"
                + "\"uuid\":\"f0918647-73a6-4001-15e8-264500000190\",\"implementationVersion\":\"1.7.0\","
                + "\"componentsVersion\":{\"os_mon\":\"2.2.5\",\"mnesia\":\"4.4.17\",\"kernel\":\"2.14.3\","
                + "\"sasl\":\"2.1.9.3\",\"ns_server\":\"1.7.0\",\"stdlib\":\"1.17.3\"}}");
        pw.flush();
        return sw.toString().getBytes();
    }




    /**
     * Program entry point
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        int port = 8091;
        int nodes = 100;
        int vbuckets = 4096;
        String harakirimonitor = null;

        Getopt getopt = new Getopt();
        getopt.addOption(new CommandLineOption('p', "--port", true)).
                addOption(new CommandLineOption('n', "--nodes", true)).
                addOption(new CommandLineOption('v', "--vbuckets", true)).
                addOption(new CommandLineOption('\0', "--harakiri-monitor", true)).
                addOption(new CommandLineOption('?', "--help", false));

        List<Entry> options = getopt.parse(args);
        for (Entry e : options) {
            if (e.key.equals("-p") || e.key.equals("--port")) {
                port = Integer.parseInt(e.value);
            } else if (e.key.equals("-n") || e.key.equals("--nodes")) {
                nodes = Integer.parseInt(e.value);
            } else if (e.key.equals("-v") || e.key.equals("--vbuckets")) {
                vbuckets = Integer.parseInt(e.value);
            } else if (e.key.equals("--harakiri-monitor")) {
                int idx = e.value.indexOf(':');
                if (idx == -1) {
                    System.err.println("ERROR: --harakiri-monitor requires host:port");
                }
                harakirimonitor = e.value;
            } else if (e.key.equals("-?") || e.key.equals("--help")) {
                System.out.println("Usage: --port=REST-port --nodes=#nodes --vbuckets=#vbuckets --harakiri-monitor=host:port");
                System.out.println("  Default values: REST-port: 8091");
                System.out.println("                  #nodes   :  100");
                System.out.println("                  #vbuckets: 4096");
                System.exit(0);
            }
        }

        try {
           CouchbaseMock mock = new CouchbaseMock(port, nodes, vbuckets);
           if (harakirimonitor != null) {
               mock.setupHarakiriMonitor(harakirimonitor);
           }
           mock.run();
        } catch (Exception e) {
            System.err.print("Fatal error! failed to create socket: ");
            System.err.println(e.getLocalizedMessage());
        }
    }



    boolean authorize(String auth) {
        if (auth == null) {
            return false;
        }

        String tokens[] = auth.split(" ");
        if (tokens.length < 3) {
            return false;
        }

        if (!tokens[1].equalsIgnoreCase("basic")) {
            return false;
        }

        String cred = Base64.decode(tokens[2]);
        int idx = cred.indexOf(":");
        if (idx == -1) {
            return false;
        }
        String user = cred.substring(0, idx);
        String passwd = cred.substring(idx + 1);

        return user.equals("Administrator") && passwd.equals("password");
    }

    @Override
    public void handleHttpRequest(HttpRequest request) {
        if (!authorize(request.getHeader("Authorization"))) {
            request.setReasonCode(HttpReasonCode.Unauthorized);
            return;
        }

        String requestedPath = request.getRequestedUri().getPath();
        if (requestedPath.equals("/pools/default/bucketsStreaming/default")) {
            try {
                // Success
                request.setReasonCode(HttpReasonCode.OK);
                request.setChunkedResponse(true);
                OutputStream os = request.getOutputStream();
                os.write(getBucketJSON());

                //this need to be to sent END marker to client
                HttpRequestImpl req = (HttpRequestImpl) request;
                req.encodeResponse();
                os = request.getOutputStream();
                os.write("\n\n\n\n".getBytes());
            } catch (IOException ex) {
                Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else if(requestedPath.equals("/pools/default/buckets/default")) {
            try {
                // Success
                request.setReasonCode(HttpReasonCode.OK);

                String output = new String(getMembaseBucketDefaultJSON());
                OutputStream os = request.getOutputStream();
                os.write(("[" + output + "]").getBytes()); //todo should be refactored (Vitaly R.)
            } catch (IOException ex) {
                Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else if ("/poolsStreaming/default".equals(requestedPath)) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                pw.print("{\"buckets\":{\"uri\":\"/pools/default/bucketsStreaming/default\"}}");
                pw.flush();
                OutputStream os = request.getOutputStream();
                os.write(sw.toString().getBytes());
            } catch (IOException ex) {
                Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else if ("/pools/default".equals(requestedPath)) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                pw.print("{\"buckets\":{\"uri\":\"/pools/default/buckets/default\"}}");
                pw.flush();
                OutputStream os = request.getOutputStream();
                os.write(sw.toString().getBytes());
            } catch (IOException ex) {
                Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else if(requestedPath.equals("/pools")) {
            try {
                // Success
                request.setReasonCode(HttpReasonCode.OK);
                OutputStream os = request.getOutputStream();
                os.write(getPoolsJSON());
            } catch (IOException ex) {
                Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else {
            // Unknown resource..
            request.setReasonCode(HttpReasonCode.Not_Found);
        }
    }

    public void failSome(float percentage) {
        for (int ii = 0; ii < servers.length; ii++) {
            if (ii % percentage == 0) {
                servers[ii].shutdown();
            }
        }
    }

    public void fixSome(float percentage) {
        for (int ii = 0; ii < servers.length; ii++) {
            if (ii % percentage == 0) {
                servers[ii].shutdown();
            }
        }
    }

    public void close() {
        httpServer.close();
    }

    @Override
    public void run() {
        List<Thread> threads = new ArrayList<Thread>();

        for (int ii = 0; ii < servers.length; ii++) {
            Thread t = new Thread(servers[ii], "mock memcached " + ii);
            t.setDaemon(true);
            t.start();
            threads.add(t);
        }

        httpServer.serve(this);

        // clear my interrupted status..
        Thread.interrupted();

        for (Thread t : threads) {
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

    public enum BucketType {
        CACHE, BASE
    }

}
