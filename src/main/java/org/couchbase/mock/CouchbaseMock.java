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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.List;
import java.util.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import org.couchbase.mock.http.HttpReasonCode;
import org.couchbase.mock.http.HttpRequest;
import org.couchbase.mock.http.HttpRequestHandler;
import org.couchbase.mock.http.HttpRequestImpl;
import org.couchbase.mock.http.HttpServer;
import org.couchbase.mock.util.Base64;
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

    private final HttpServer httpServer;
    private final Map<String, Bucket> buckets;
    private Credentials requiredHttpAuthorization;
    private static final String poolName = "default";

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, CouchbaseMock.BucketType type) throws IOException {
        Bucket bucket = null;
        switch (type) {
            case CACHE:
                bucket = new CacheBucket("default", hostname, port, numNodes, bucketStartPort, numVBuckets);
                break;
            case BASE:
                bucket = new MembaseBucket("default", hostname, port, numNodes, bucketStartPort, numVBuckets);
                break;
            default:
                throw new FileNotFoundException("I don't know about this type...");
        }

        buckets = new HashMap<String, Bucket>();
        buckets.put("default", bucket);

        httpServer = new HttpServer(port);
        requiredHttpAuthorization = new Credentials("Administrator", "password");
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int numVBuckets, CouchbaseMock.BucketType type) throws IOException {
        this(hostname, port, numNodes, 0, numVBuckets, type);
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int numVBuckets) throws IOException {
        this(hostname, port, numNodes, numVBuckets, BucketType.BASE);
    }

    /**
     * The port of the http server providing the REST interface.
     */
    public int getHttpPort() {
        return httpServer.getPort();
    }

    public Credentials getRequiredHttpAuthorization() {
        return requiredHttpAuthorization;
    }

    /**
     * Set the required http basic authorization. To have no http auth at all, just provide
     * <code>null</code>.
     * @param requiredHttpAuthorization the credentials that need to be passed as Authorization header
     *  (basic auth) when accessing the REST interface, or <code>null</code> if no http auth is wanted.
     */
    public void setRequiredHttpAuthorization(Credentials requiredHttpAuthorization) {
        this.requiredHttpAuthorization = requiredHttpAuthorization;
    }

    private byte[] getPoolsJSON() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{\"pools\":[{\"name\":\"" + poolName + "\",\"uri\":\"/pools/" + poolName +"\","
                + "\"streamingUri\":\"/poolsStreaming/"+ poolName +"\"}],\"isAdminCreds\":true,"
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
        String hostname = null;

        Getopt getopt = new Getopt();
        getopt.addOption(new CommandLineOption('h', "--host", true)).
                addOption(new CommandLineOption('p', "--port", true)).
                addOption(new CommandLineOption('n', "--nodes", true)).
                addOption(new CommandLineOption('v', "--vbuckets", true)).
                addOption(new CommandLineOption('\0', "--harakiri-monitor", true)).
                addOption(new CommandLineOption('?', "--help", false));

        List<Entry> options = getopt.parse(args);
        for (Entry e : options) {
            if (e.key.equals("-h") || e.key.equals("--host")) {
                hostname = e.value;
            } else if (e.key.equals("-p") || e.key.equals("--port")) {
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
                System.out.println("Usage: --host=hostname --port=REST-port --nodes=#nodes --vbuckets=#vbuckets --harakiri-monitor=host:port");
                System.out.println("  Default values: REST-port: 8091");
                System.out.println("                  #nodes   :  100");
                System.out.println("                  #vbuckets: 4096");
                System.exit(0);
            }
        }

        try {
           CouchbaseMock mock = new CouchbaseMock(hostname, port, nodes, vbuckets);
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
        if (requiredHttpAuthorization == null) {
            return true;
        }
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

        return requiredHttpAuthorization.matches(user, passwd);
    }

    private boolean handlePoolsRequest(HttpRequest request, String[] tokens) {
        if (tokens.length == 2) {
            // GET /pools
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
            return true;
        } else {
            if (!poolName.equals(tokens[2])) {
                return false; // unknown pool
            }

            if (tokens.length == 3) {
                // GET /pools/:poolName
                try {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    pw.print("{\"buckets\":{\"uri\":\"/pools/" + poolName + "/buckets/default\"}}");
                    pw.flush();
                    OutputStream os = request.getOutputStream();
                    os.write(sw.toString().getBytes());
                } catch (IOException ex) {
                    Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                    request.resetResponse();
                    request.setReasonCode(HttpReasonCode.Internal_Server_Error);
                }
                return true;
            }

            Bucket bucket;
            if ("buckets".equals(tokens[3])) {
                try {
                    OutputStream os = request.getOutputStream();
                    if (tokens.length == 5 && (bucket = buckets.get(tokens[4])) != null) {
                        // GET /pools/:poolName/buckets/:bucketName
                        request.setReasonCode(HttpReasonCode.OK);
                        os.write(bucket.getJSON().getBytes()); //todo should be refactored (Vitaly R.)
                    } else {
                        // GET /pools/:poolName/buckets
                        os.write(("[").getBytes());
                        for (Bucket bb : buckets.values()) {
                            os.write(bb.getJSON().getBytes());
                        }
                        os.write(("]" ).getBytes());
                    }
                }
                catch (IOException ex) {
                    Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                    request.resetResponse();
                    request.setReasonCode(HttpReasonCode.Internal_Server_Error);
                }
                return true;
            } else if ("bucketsStreaming".equals(tokens[3]) && tokens.length == 5 && (bucket = buckets.get(tokens[4])) != null) {
                // GET /pools/:poolName/bucketsStreaming/:bucketName
                try {
                    // Success
                    request.setReasonCode(HttpReasonCode.OK);
                    request.setChunkedResponse(true);
                    OutputStream os = request.getOutputStream();
                    os.write(bucket.getJSON().getBytes());

                    //this need to be to sent END marker to client
                    HttpRequestImpl req = (HttpRequestImpl) request;
                    req.encodeResponse();
                    os = request.getOutputStream();
                    os.write("\n\n\n\n".getBytes());
                }
                catch (IOException ex) {
                    Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
                    request.resetResponse();
                    request.setReasonCode(HttpReasonCode.Internal_Server_Error);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void handleHttpRequest(HttpRequest request) {
        if (!authorize(request.getHeader("Authorization"))) {
            request.setReasonCode(HttpReasonCode.Unauthorized);
            return;
        }

        String requestedPath = request.getRequestedUri().getPath();

        String[] tokens = requestedPath.split("/");

        if (tokens.length > 1 && tokens[0].length() == 0) {
            if ("pools".equals(tokens[1])) {
                if (handlePoolsRequest(request, tokens)) {
                    return ;
                }
            }
        }

        request.setReasonCode(HttpReasonCode.Not_Found);
    }

    public void failSome(String name, float percentage) {
        Bucket bucket = buckets.get(name);
        if (bucket != null) {
            bucket.failSome(percentage);
        }

    }

    public void fixSome(String name, float percentage) {
        Bucket bucket = buckets.get(name);
        if (bucket != null) {
            bucket.fixSome(percentage);
        }
    }

    public void close() {
        httpServer.close();
    }

    @Override
    public void run() {
        List<Thread> threads = new ArrayList<Thread>();
        for (String s : buckets.keySet()) {
            Bucket bucket = buckets.get(s);
            bucket.start(threads);
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

    public static class Credentials {
        private String username;
        private String password;
        public Credentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
        boolean matches(String username, String password) {
            return equals(new Credentials(username, password));
        }
        public String getUsername() {
            return username;
        }
        public String getPassword() {
            return password;
        }
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((password == null) ? 0 : password.hashCode());
            result = prime * result + ((username == null) ? 0 : username.hashCode());
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Credentials other = (Credentials) obj;
            if (password == null) {
                if (other.password != null) {
                    return false;
                }
            } else if (!password.equals(other.password)) {
                return false;
            }
            if (username == null) {
                if (other.username != null) {
                    return false;
                }
            } else if (!username.equals(other.username)) {
                return false;
            }
            return true;
        }
    }

}
