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
package org.membase.jmembase;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.membase.jmembase.http.HttpRequest;
import org.membase.jmembase.util.JSON;
import org.membase.jmembase.memcached.DataStore;
import org.membase.jmembase.memcached.MemcachedServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.membase.jmembase.http.HttpReasonCode;
import org.membase.jmembase.http.HttpRequestHandler;
import org.membase.jmembase.http.HttpServer;
import org.membase.jmembase.util.Base64;

/**
 * This is a super-scaled down version of something that might look like
 * membase ;-) It provides the REST interface to our bucket lists, so that
 * you may use it to retrieve a list of servers and where their vbuckets
 * are...
 *
 * @author Trond Norbye
 */
public class JMembase implements HttpRequestHandler, Runnable {

    private final DataStore datastore;
    private final MemcachedServer servers[];
    private final int numVBuckets;
    private final HttpServer httpServer;

    public JMembase(int port, int numNodes, int numVBuckets) throws IOException {
        this.numVBuckets = numVBuckets;
        datastore = new DataStore(numVBuckets);
        servers = new MemcachedServer[numNodes];
        for (int ii = 0; ii < servers.length; ii++) {
            servers[ii] = new MemcachedServer(0, datastore);
        }

        // Let's start distribute the vbuckets across the servers
        Random random = new Random();
        for (int ii = 0; ii < numVBuckets; ++ii) {
            int idx = random.nextInt(servers.length);
            datastore.setOwnership(ii, servers[idx]);
        }

        httpServer = new HttpServer(port);
    }

    private byte[] getBucketJSON() {
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

    /**
     * Program entry point
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        JMembase membase = null;
        try {
            membase = new JMembase(8091, 100, 4096);
        } catch (Exception e) {
            System.err.print("Fatal error! failed to create socket: ");
            System.err.println(e.getLocalizedMessage());
        }
        membase.run();
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
//        if (!authorize(request.getHeader("Authorization"))) {
//            request.setReasonCode(HttpReasonCode.Unauthorized);
//            return;
//        }

	String requestedPath = request.getRequestedUri().getPath();
        if (requestedPath.equals("/pools/default/bucketsStreaming/default")) {
            try {
                // Success
                request.setReasonCode(HttpReasonCode.OK);
                request.setChunkedResponse(true);
                OutputStream os = request.getOutputStream();
                os.write(getBucketJSON());
            } catch (IOException ex) {
                Logger.getLogger(JMembase.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else if(requestedPath.equals("/pools/default/buckets/default")) {
            try {
                // Success
                request.setReasonCode(HttpReasonCode.OK);
                OutputStream os = request.getOutputStream();
                os.write(getBucketJSON());
            } catch (IOException ex) {
                Logger.getLogger(JMembase.class.getName()).log(Level.SEVERE, null, ex);
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
                Logger.getLogger(JMembase.class.getName()).log(Level.SEVERE, null, ex);
                request.resetResponse();
                request.setReasonCode(HttpReasonCode.Internal_Server_Error);
            }
        } else {
            // Unknown resource..
            request.setReasonCode(HttpReasonCode.Not_Found);
        }
    }

    public void close() {
        httpServer.close();
    }

    @Override
    public void run() {
        List<Thread> threads = new ArrayList<Thread>();

        for (int ii = 0; ii < servers.length; ii++) {
            Thread t = new Thread(servers[ii]);
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
                    Logger.getLogger(JMembase.class.getName()).log(Level.SEVERE, null, ex);
                    t.interrupt();
                }
            } while (t != null);
        }
    }
}
