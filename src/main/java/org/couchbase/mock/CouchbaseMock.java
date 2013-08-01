/**
 *     Copyright 2013 Couchbase, Inc.
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

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.List;
import java.util.logging.Logger;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.couchbase.mock.harakiri.HarakiriMonitor;
import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.control.MockCommandDispatcher;
import org.couchbase.mock.http.Authenticator;
import org.couchbase.mock.http.ControlHandler;
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

    private final Map<String, Bucket> buckets = new HashMap<String, Bucket>();
    private int port = 8091;
    private HttpServer httpServer;
    private Authenticator authenticator;
    private ArrayList<Thread> nodeThreads;
    private final CountDownLatch startupLatch = new CountDownLatch(1);
    private HarakiriMonitor harakiriMonitor;
    private MockCommandDispatcher controlDispatcher;

    public void setupHarakiriMonitor(String host, boolean terminate) throws IOException {
        int idx = host.indexOf(':');
        String h = host.substring(0, idx);
        int p = Integer.parseInt(host.substring(idx + 1));
        harakiriMonitor = new HarakiriMonitor(h, p, terminate, controlDispatcher);
        harakiriMonitor.start();
    }

    /**
     * @return the poolName
     */
    @SuppressWarnings("SameReturnValue")
    public String getPoolName() {
        return "default";
    }

    /**
     * @return the buckets
     */
    public Map<String, Bucket> getBuckets() {
        return buckets;
    }

    public HarakiriMonitor getMonitor() {
        return harakiriMonitor;
    }

    public MockCommandDispatcher getDispatcher() {
        return controlDispatcher;
    }

    private static BucketConfiguration parseBucketString(String spec, BucketConfiguration defaults)
    {
        BucketConfiguration config = new BucketConfiguration(defaults);

        String[] parts = spec.split(":");
        String name = parts[0];
        String pass = "";

        config.name = name;
        if (parts.length > 1) {
            pass = parts[1];
            if (parts.length > 2) {
                if (parts[2].equals("memcache")) {
                    config.type = BucketType.MEMCACHED;
                }
            }
        }
        config.password = pass;
        return config;
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, String bucketSpec, int numReplicas) throws IOException {
        List<BucketConfiguration> configs = new ArrayList<BucketConfiguration>();
        BucketConfiguration defaultConfig = new BucketConfiguration();

        defaultConfig.name = "default";
        defaultConfig.type = BucketType.COUCHBASE;
        defaultConfig.hostname = hostname;
        defaultConfig.port = port;
        defaultConfig.numNodes = numNodes;
        if (numReplicas > -1) {
            defaultConfig.numReplicas = numReplicas;
        }

        defaultConfig.bucketStartPort = bucketStartPort;
        defaultConfig.numVBuckets = numVBuckets;
        if (bucketSpec == null) {
            configs.add(defaultConfig);
        } else {
            for (String spec : bucketSpec.split(",")) {
                configs.add(parseBucketString(spec, defaultConfig));
            }
        }

        initCommon(port, configs);
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets) throws IOException {
        this(hostname, port, numNodes, bucketStartPort, numVBuckets, null, -1);
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int numVBuckets) throws IOException {
        this(hostname, port, numNodes, 0, numVBuckets, null, -1);
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int numVBuckets, String bucketSpec) throws IOException {
        this(hostname, port, numNodes, 0, numVBuckets, bucketSpec, -1);
    }

    public CouchbaseMock(int port, List<BucketConfiguration> configs) throws IOException {
        initCommon(port, configs);
    }

    private void initCommon(int port, List<BucketConfiguration> configs) throws IOException {
        this.port = port;
        for (BucketConfiguration config : configs) {
            Bucket bucket = Bucket.create(this, config);
            buckets.put(bucket.getName(), bucket);
        }
        authenticator = new Authenticator("Administrator", "password", buckets);
        controlDispatcher = new MockCommandDispatcher(this);
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
     *
     * @param authenticator the credentials that need to be passed as Authorization header
     *                      (basic auth) when accessing the REST interface, or <code>null</code>
     *                      if no http auth is wanted.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    /**
     * Program entry point
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        int port = 8091;
        int nodes = 100;
        int vbuckets = 4096;
        String harakiriMonitorAddress = null;
        String hostname = null;
        String bucketsSpec = null;
        int replicaCount = -1;

        Getopt getopt = new Getopt();
        getopt.addOption(new CommandLineOption('h', "--host", true)).
                addOption(new CommandLineOption('b', "--buckets", true)).
                addOption(new CommandLineOption('p', "--port", true)).
                addOption(new CommandLineOption('n', "--nodes", true)).
                addOption(new CommandLineOption('v', "--vbuckets", true)).
                addOption(new CommandLineOption('\0', "--harakiri-monitor", true)).
                addOption(new CommandLineOption('R', "--replicas", true)).
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
            } else if (e.key.equals("-R") || e.key.equals("--replicas")) {
                replicaCount = Integer.parseInt(e.value);
            } else if (e.key.equals("--harakiri-monitor")) {
                int idx = e.value.indexOf(':');
                if (idx == -1) {
                    System.err.println("ERROR: --harakiri-monitor requires host:port");
                }
                harakiriMonitorAddress = e.value;
            } else if (e.key.equals("-?") || e.key.equals("--help")) {
                System.out.println("Usage: --host=hostname --buckets=bucketsSpec --port=REST-port --nodes=#nodes --vbuckets=#vbuckets --harakiri-monitor=host:port --replicas=#replicas");
                System.out.println("  Default values: REST-port:    8091");
                System.out.println("                  bucketsSpec:  default:");
                System.out.println("                  #nodes:       10");
                System.out.println("                  #vbuckets:    4096");
                System.out.println("                  #replicas:    2");
                System.out.println("Buckets descriptions is a comma-separated list of {name}:{password}:{bucket type} pairs. "
                        + "To allow unauthorized connections, omit password. "
                        + "Third parameter could be either 'memcache' or 'couchbase' (default value is 'couchbase'). E.g.\n"
                        + "    default:,test:,protected:secret,cache::memcache");
                System.exit(0);
            }
        }

        try {
            CouchbaseMock mock = new CouchbaseMock(hostname, port, nodes, 0, vbuckets, bucketsSpec, replicaCount);
            if (harakiriMonitorAddress != null) {
                mock.setupHarakiriMonitor(harakiriMonitorAddress, true);
            }
            mock.start();
        } catch (Exception e) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, "Fatal error! failed to create socket: ", e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public void failSome(String name, float percentage) {
        Bucket bucket = getBuckets().get(name);
        if (bucket != null) {
            bucket.failSome(percentage);
        }

    }

    @SuppressWarnings("UnusedDeclaration")
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
            boolean busy = true;
            do {
                if (port == 0) {
                    ServerSocket server = new ServerSocket(0);
                    port = server.getLocalPort();
                    server.close();
                }
                try {
                    httpServer = HttpServer.create(new InetSocketAddress(port), 10);
                    busy = false;
                } catch (BindException ex) {
                    System.err.println("Looks like port " + port + " busy, lets try another one");
                }
            } while (busy);
            httpServer.createContext("/pools", new PoolsHandler(this)).setAuthenticator(authenticator);
            httpServer.createContext("/mock", new ControlHandler(controlDispatcher));
            httpServer.setExecutor(Executors.newCachedThreadPool());
            httpServer.start();
            startupLatch.countDown();
        } catch (IOException ex) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

    }
}
