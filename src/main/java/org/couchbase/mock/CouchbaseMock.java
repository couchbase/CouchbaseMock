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

import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.List;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.http.Authenticator;
import org.couchbase.mock.http.PoolsHandler;
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
public class CouchbaseMock {

    private final Map<String, Bucket> buckets;
    private final String poolName = "default";
    private int port = 8091;
    private HttpServer httpServer;
    private Authenticator authenticator;
    private ArrayList<Thread> nodeThreads;
    private final Lock configLock = new ReentrantLock();
    private Condition configInSync = configLock.newCondition();
    private final CountDownLatch startupLatch;

    private void setupHarakiriMonitor(String host) {
        int idx = host.indexOf(':');
        String h = host.substring(0, idx);
        int p = Integer.parseInt(host.substring(idx + 1));
        try {
            HarakiriMonitor m = new HarakiriMonitor(h, p, true, this);
            Thread t = new Thread(m, "HarakiriMonitor");
            t.start();
        } catch (Throwable t) {
            System.err.println("Failed to set up harakiri monitor: " + t.getMessage());
            System.exit(1);
        }
    }

    /**
     * @return the poolName
     */
    public String getPoolName() {
        return poolName;
    }

    /**
     * @return the buckets
     */
    public Map<String, Bucket> getBuckets() {
        return buckets;
    }

    private void failover(String bucketName, int idx) {
        Bucket bucket = buckets.get(bucketName);
        if (bucket != null) {
            bucket.failover(idx);
        }
    }

    private void respawn(String bucketName, int idx) {
        Bucket bucket = buckets.get(bucketName);
        if (bucket != null) {
            bucket.respawn(idx);
        }
    }

    public static class HarakiriMonitor implements Runnable {

        private final boolean terminate;
        private final Socket sock;
        private final BufferedReader input;
        private final CouchbaseMock mock;
        private final OutputStream output;

        public HarakiriMonitor(String host, int port, boolean terminate, CouchbaseMock mock) throws IOException {
            this.mock = mock;
            this.terminate = terminate;
            sock = new Socket(host, port);
            input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            output = sock.getOutputStream();
        }

        @Override
        public void run() {
            boolean closed = false;
            String[] tokens;
            String command, bucket, packet;
            int idx;
            String http = "" + mock.getHttpPort() + '\0';
            try {
                mock.waitForStartup();
                output.write(http.getBytes());
                output.flush();
            } catch (InterruptedException ex) {
                closed = true;
            } catch (IOException ex) {
                closed = true;
            }
            while (!closed) {
                try {
                    packet = input.readLine();
                    if (packet == null) {
                        closed = true;
                    } else if (mock != null) {
                        /* format (bucket name is optional):
                         *
                         *     failover,123,default
                         *     respawn,123,default
                         */
                        tokens = packet.split(",");
                        if (tokens.length >= 2) {
                            try {
                                command = tokens[0];
                                idx = Integer.parseInt(tokens[1].trim());
                                if (tokens.length == 3) {
                                    bucket = tokens[2];
                                } else {
                                    bucket = "default";
                                }
                                if ("failover".equals(command)) {
                                    mock.failover(bucket, idx);
                                } else if ("respawn".equals(command)) {
                                    mock.respawn(bucket, idx);
                                }
                            } catch (NumberFormatException ex) {
                            }
                        }
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

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, String bucketSpec) throws IOException {
        startupLatch = new CountDownLatch(1);
        buckets = new HashMap<String, Bucket>();
        try {
            Bucket bucket;

            if (bucketSpec == null) {
                bucket = Bucket.create(BucketType.COUCHBASE, "default", hostname, port, numNodes, bucketStartPort, numVBuckets, this, "");
                buckets.put("default", bucket);
            } else {
                for (String spec : bucketSpec.split(",")) {
                    String[] parts = spec.split(":");
                    String name = parts[0], pass = "";
                    BucketType type = BucketType.COUCHBASE;
                    if (parts.length > 1) {
                        pass = parts[1];
                        if (parts.length > 2 && "memcache".equals(parts[2])) {
                            type = BucketType.MEMCACHE;
                        }
                    }
                    bucket = Bucket.create(type, name, hostname, port, numNodes, bucketStartPort, numVBuckets, this, pass);
                    buckets.put(name, bucket);
                }
            }

            this.port = port;
            authenticator = new Authenticator("Administrator", "password", buckets);
        } catch (SecurityException ex) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets) throws IOException {
        this(hostname, port, numNodes, bucketStartPort, numVBuckets, null);
    }
    public CouchbaseMock(String hostname, int port, int numNodes, int numVBuckets) throws IOException {
        this(hostname, port, numNodes, 0, numVBuckets, null);
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int numVBuckets, String bucketSpec) throws IOException {
        this(hostname, port, numNodes, 0, numVBuckets, bucketSpec);
    }

    public void waitForStartup() throws InterruptedException {
        startupLatch.await();
    }

    /**
     * The port of the http server providing the REST interface.
     */
    public int getHttpPort() {
        return port;
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * Set the required http basic authorization. To have no http auth at all, just provide
     * <code>null</code>.
     * @param requiredHttpAuthorization the credentials that need to be passed as Authorization header
     *  (basic auth) when accessing the REST interface, or <code>null</code> if no http auth is wanted.
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
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
        String bucketsSpec = null;

        Getopt getopt = new Getopt();
        getopt.addOption(new CommandLineOption('h', "--host", true)).
                addOption(new CommandLineOption('b', "--buckets", true)).
                addOption(new CommandLineOption('p', "--port", true)).
                addOption(new CommandLineOption('n', "--nodes", true)).
                addOption(new CommandLineOption('v', "--vbuckets", true)).
                addOption(new CommandLineOption('\0', "--harakiri-monitor", true)).
                addOption(new CommandLineOption('?', "--help", false));

        List<Entry> options = getopt.parse(args);
        for (Entry e : options) {
            if (e.key.equals("-h") || e.key.equals("--host")) {
                hostname = e.value;
            } else if (e.key.equals("-b") || e.key.equals("--buckets")) {
                bucketsSpec = e.value;
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
                System.out.println("Usage: --host=hostname --buckets=bucketsSpec --port=REST-port --nodes=#nodes --vbuckets=#vbuckets --harakiri-monitor=host:port");
                System.out.println("  Default values: REST-port:    8091");
                System.out.println("                  bucketsSpec:  default:");
                System.out.println("                  #nodes:       100");
                System.out.println("                  #vbuckets:    4096");
                System.out.println("Buckets descriptions is a comma-separated list of {name}:{password}:{bucket type} pairs. "
                        + "To allow unauthorized connections, omit password. "
                        + "Third parameter could be either 'memcache' or 'couchbase' (default value is 'couchbase'). E.g.\n"
                        + "    default:,test:,protected:secret,cache::memcache");
                System.exit(0);
            }
        }

        try {
            if (port == 0) {
                ServerSocket server = new ServerSocket(0);
                port = server.getLocalPort();
                server.close();
            }
            CouchbaseMock mock = new CouchbaseMock(hostname, port, nodes, vbuckets, bucketsSpec);
            if (harakirimonitor != null) {
                mock.setupHarakiriMonitor(harakirimonitor);
            }
            mock.start();
        } catch (Exception e) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, "Fatal error! failed to create socket: ", e);
        }
    }

    public void configUpdated() {
        configLock.lock();
        try {
            configInSync.signalAll();
        } finally {
            configLock.unlock();
        }
    }

    public boolean waitForUpdate() {
        configLock.lock();
        try {
            configInSync.await();
        } catch (InterruptedException ex) {
            return false;
        } finally {
            configLock.unlock();
        }
        return true;
    }

    public void failSome(String name, float percentage) {
        Bucket bucket = getBuckets().get(name);
        if (bucket != null) {
            bucket.failSome(percentage);
        }

    }

    public void fixSome(String name, float percentage) {
        Bucket bucket = getBuckets().get(name);
        if (bucket != null) {
            bucket.fixSome(percentage);
        }
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }

        for (Thread t : nodeThreads) {
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

    /*
     * Start cluster in background
     */
    public void start() {
        nodeThreads = new ArrayList<Thread>();
        for (String s : getBuckets().keySet()) {
            Bucket bucket = getBuckets().get(s);
            bucket.start(nodeThreads);
        }

        try {
            httpServer = HttpServer.create(new InetSocketAddress(port), 10);
            httpServer.createContext("/pools", new PoolsHandler(this)).setAuthenticator(authenticator);
            httpServer.setExecutor(Executors.newCachedThreadPool());
            httpServer.start();
            startupLatch.countDown();
        } catch (IOException ex) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
