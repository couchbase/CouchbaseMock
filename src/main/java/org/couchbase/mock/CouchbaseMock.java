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

import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.client.RestAPIUtil;
import org.couchbase.mock.control.MockCommandDispatcher;
import org.couchbase.mock.harakiri.HarakiriMonitor;
import org.couchbase.mock.http.*;
import org.couchbase.mock.http.capi.CAPIServer;
import org.couchbase.mock.httpio.HttpServer;
import org.couchbase.mock.util.Getopt;
import org.couchbase.mock.util.Getopt.CommandLineOption;
import org.couchbase.mock.util.Getopt.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a super-scaled down version of something that might look like
 * membase ;-) It provides the REST interface to our bucket lists, so that
 * you may use it to retrieve a list of servers and where their vbuckets
 * are...
 *
 * @author Trond Norbye
 */
public class CouchbaseMock {
    private final Map<String,BucketConfiguration> initialConfigs;
    private final Map<String, Bucket> buckets = new HashMap<String, Bucket>();
    private final HttpServer httpServer;
    private final Authenticator authenticator;
    private final CountDownLatch startupLatch = new CountDownLatch(1);
    private final MockCommandDispatcher controlDispatcher;
    private final PoolsHandler poolsHandler;
    private BucketConfiguration defaultConfig = new BucketConfiguration();

    private int port = 8091;
    private volatile String host = "127.0.0.1";
    private HarakiriMonitor harakiriMonitor;

    public void setupHarakiriMonitor(String host, boolean terminate) throws IOException {
        int idx = host.indexOf(':');
        String h = host.substring(0, idx);
        int p = Integer.parseInt(host.substring(idx + 1));

        if (terminate) {
            harakiriMonitor.setTemrinateAction(new Callable() {
                @Override
                public Object call() throws Exception {
                    System.exit(1);
                    return null;
                }
            });
        }

        harakiriMonitor.bind(h, p);
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

    Map<String,BucketConfiguration> getInitialConfigs() {
        return initialConfigs;
    }

    public void clearInitialConfigs() {
        if (!buckets.isEmpty()) {
            throw new IllegalStateException("Cannot clear initial configs once they have been started");
        }
        initialConfigs.clear();
    }

    public HarakiriMonitor getMonitor() {
        return harakiriMonitor;
    }

    public MockCommandDispatcher getDispatcher() {
        return controlDispatcher;
    }

    public PoolsHandler getPoolsHandler() {
        return poolsHandler;
    }

    public BucketConfiguration getDefaultConfig() {
        return new BucketConfiguration(defaultConfig);
    }

    private static BucketConfiguration parseBucketString(String spec, BucketConfiguration defaults) {
        BucketConfiguration config = new BucketConfiguration(defaults);

        String[] parts = spec.split(":");
        String name = parts[0];
        String pass = "";

        config.name = name;
        if (parts.length > 1) {
            pass = parts[1];
            if (parts.length > 2) {
                if (parts[2].startsWith("memcache")) {
                    config.type = BucketType.MEMCACHED;
                }
            }
        }
        config.password = pass;
        return config;
    }

    private static BucketConfiguration createDefaultConfig(String hostname, int numNodes, int port, int bucketStartPort, int numVBuckets, int numReplicas) {
        BucketConfiguration defaultConfig = new BucketConfiguration();
        defaultConfig.type = BucketType.COUCHBASE;
        defaultConfig.hostname = hostname;
        defaultConfig.port = port;
        defaultConfig.numNodes = numNodes;
        if (numReplicas > -1) {
            defaultConfig.numReplicas = numReplicas;
        }

        defaultConfig.bucketStartPort = bucketStartPort;
        defaultConfig.numVBuckets = numVBuckets;
        return defaultConfig;
    }

    private static List<BucketConfiguration> fromSpecString(String bucketSpec, BucketConfiguration defaultConfig) {
        List<BucketConfiguration> configs = new ArrayList<BucketConfiguration>();
        if (bucketSpec != null) {
            for (String spec : bucketSpec.split(",")) {
                configs.add(parseBucketString(spec, defaultConfig));
            }
        }
        if (configs.isEmpty()) {
            BucketConfiguration defaultBucket = new BucketConfiguration(defaultConfig);
            defaultBucket.name = "default";
            configs.add(defaultBucket);
        }
        return configs;
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, String bucketSpec, int numReplicas) throws IOException {
        this(port, fromSpecString(bucketSpec, createDefaultConfig(hostname, numNodes, port, bucketStartPort, numVBuckets, numReplicas)));
        defaultConfig = createDefaultConfig(hostname, numNodes, port, bucketStartPort, numVBuckets, numReplicas);
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
        this.port = port;

        authenticator = new Authenticator("Administrator", "password");
        controlDispatcher = new MockCommandDispatcher(this);
        initialConfigs = new HashMap<String, BucketConfiguration>();
        harakiriMonitor = new HarakiriMonitor(controlDispatcher);
        httpServer = new HttpServer();

        for (BucketConfiguration config : configs) {
            initialConfigs.put(config.name, config);
        }

        poolsHandler = new PoolsHandler(this);
        poolsHandler.register(httpServer);
        httpServer.register("/mock/*", new ControlHandler(controlDispatcher));
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
    public String getHttpHost() { return host; }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * Program entry point
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        BucketConfiguration defaultConfig = new BucketConfiguration();
        int port = 8091;
        int nodes = defaultConfig.numNodes;
        int vbuckets = defaultConfig.numVBuckets;
        String harakiriMonitorAddress = null;
        String hostname = null;
        String bucketsSpec = null;
        String docsFile = null;
        boolean useBeerSample = false;
        boolean emptyCluster = false;
        int replicaCount = -1;

        Getopt getopt = new Getopt();
        getopt.addOption(new CommandLineOption('h', "--host", true)).
                addOption(new CommandLineOption('b', "--buckets", true)).
                addOption(new CommandLineOption('p', "--port", true)).
                addOption(new CommandLineOption('n', "--nodes", true)).
                addOption(new CommandLineOption('v', "--vbuckets", true)).
                addOption(new CommandLineOption('\0', "--harakiri-monitor", true)).
                addOption(new CommandLineOption('R', "--replicas", true)).
                addOption(new CommandLineOption('D', "--docs", true)).
                addOption(new CommandLineOption('S', "--with-beer-sample", false)).
                addOption(new CommandLineOption('E', "--empty", false)).
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
            } else if (e.key.equals("-D") || e.key.equals("--docs")) {
                docsFile = e.value;
            } else if (e.key.equals("-S") || e.key.equals("--with-beer-sample")) {
                useBeerSample = true;
            } else if (e.key.equals("-E") || e.key.equals("--empty")) {
                emptyCluster = true;
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

            if (emptyCluster) {
                mock.clearInitialConfigs();
            }

            mock.start();
            // See if we need to load documents:
            if (docsFile != null) {
                DocumentLoader loader = new DocumentLoader(mock, "default");
                loader.loadDocuments(docsFile);
            } else if (useBeerSample) {
                RestAPIUtil.loadBeerSample(mock);
            }

        } catch (Exception e) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, "Fatal error! failed to create socket: ", e);
        }
    }

    public void stop() {
        httpServer.stopServer();
        for (Bucket bucket : buckets.values()) {
            bucket.stop();
        }
    }

    public void createBucket(BucketConfiguration config) throws BucketAlreadyExistsException, IOException {
        if (!config.validate()) {
            throw new IllegalArgumentException("Invalid bucket configuration");
        }

        synchronized (buckets) {
            if (buckets.containsKey(config.name)) {
                throw new BucketAlreadyExistsException(config.name);
            }

            Bucket bucket = Bucket.create(this, config);
            BucketAdminServer adminServer = new BucketAdminServer(bucket, httpServer, this);
            adminServer.register();
            bucket.setAdminServer(adminServer);

            HttpAuthVerifier verifier = new HttpAuthVerifier(bucket, authenticator);

            if (config.type == BucketType.COUCHBASE) {
                CAPIServer capi = new CAPIServer(bucket, verifier);
                capi.register(httpServer);
                bucket.setCAPIServer(capi);
            }

            buckets.put(config.name, bucket);
            bucket.start();
        }
    }

    public void removeBucket(String name) throws FileNotFoundException {
        Bucket bucket;
        synchronized (buckets) {
            if (!buckets.containsKey(name)) {
                throw new FileNotFoundException("No such bucket: "+ name);
            }
            bucket = buckets.remove(name);
        }
        CAPIServer capi = bucket.getCAPIServer();
        if (capi != null) {
            capi.shutdown();
        }
        BucketAdminServer adminServer = bucket.getAdminServer();
        if (adminServer != null) {
            adminServer.shutdown();
        }
        bucket.stop();
    }

    /*
     * Start cluster in background
     */
    public void start() {
        try {
            if (port == 0) {
                ServerSocketChannel ch = ServerSocketChannel.open();
                ch.socket().bind(new InetSocketAddress(0));
                port = ch.socket().getLocalPort();
                httpServer.bind(ch);
            } else {
                httpServer.bind(new InetSocketAddress(port));
            }
        } catch (IOException ex) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        for (BucketConfiguration config : initialConfigs.values()) {
            try {
                createBucket(config);
            } catch (IOException ex) {
                ex.printStackTrace();
            } catch (BucketAlreadyExistsException ex) {
                ex.printStackTrace();
            }
        }

        httpServer.start();
        startupLatch.countDown();
    }
}
