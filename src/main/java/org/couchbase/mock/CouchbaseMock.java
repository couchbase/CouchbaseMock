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
import org.couchbase.mock.http.query.QueryServer;
import org.couchbase.mock.httpio.HttpServer;
import org.couchbase.mock.util.Getopt;
import org.couchbase.mock.util.Getopt.CommandLineOption;
import org.couchbase.mock.util.Getopt.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is the main entry point to the Mock cluster. It represents a "Cluster"
 * of sorts.
 *
 *
 * Unlike in a real cluster, the mock "Nodes" do not support multi-tenancy, or in
 * other words, a single "Node" can only serve a single bucket. From a client
 * perspective this should not matter, but it is important to keep this aspect in
 * mind.
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
    private HarakiriMonitor harakiriMonitor;

    /**
     * Tell the harakiri monitor to connect to the given address.
     * @param address The address the monitor should connect to
     * @param terminate Whether the application should exit when a disconnect is detected on the socket
     * @throws IOException If the monitor could not listen on the given port, or if the monitor is already listening
     */
    public void startHarakiriMonitor(InetSocketAddress address, boolean terminate) throws IOException {
        if (terminate) {
            harakiriMonitor.setTemrinateAction(new Callable() {
                @Override
                public Object call() throws Exception {
                    System.exit(1);
                    return null;
                }
            });
        }

        harakiriMonitor.connect(address.getHostName(), address.getPort());
        harakiriMonitor.start();
    }

    /**
     * Start the monitor
     * @param host A string in the form of {@code host:port}
     * @param terminate Whether the application should terminate on disconnect
     * @throws IOException
     * @see {@link #startHarakiriMonitor(java.net.InetSocketAddress, boolean)}
     */
    public void startHarakiriMonitor(String host, boolean terminate) throws IOException {
        int idx = host.indexOf(':');
        String h = host.substring(0, idx);
        int p = Integer.parseInt(host.substring(idx + 1));
        startHarakiriMonitor(new InetSocketAddress(h, p), terminate);
    }

    public String getPoolName() {
        return "default";
    }

    /**
     * Return the list of active buckets for inspection. The returned value should not be modified.
     * Use {@link #createBucket(BucketConfiguration)} or {@link #removeBucket(String)} to add or
     * remove buckets
     */
    public Map<String, Bucket> getBuckets() {
        return Collections.unmodifiableMap(buckets);
    }

    // Used by tests.
    Map<String,BucketConfiguration> getInitialConfigs() {
        return initialConfigs;
    }

    /**
     * Clear the initial bucket configurations which were added during construction. This should be used
     * if you want {@link #start()} to start up a blank cluster.
     */
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

    /**
     * Get the default configuration for buckets. The default configuration is determined by values
     * passed to the constructor.
     * @return A copy of the default configuration. This may be passed as the first argument to
     * {@link org.couchbase.mock.BucketConfiguration}
     */
    public BucketConfiguration getDefaultConfig() {
        return new BucketConfiguration(defaultConfig);
    }

    /**
     * Parses the "Bucket specification string" (typically supplied on the command line) into a list of buckets
     * @param bucketSpec The specification string specified
     * @param defaultConfig The default configuration used to supplant non-specified values
     * @return A list of initial configurations
     */
    private static List<BucketConfiguration> fromSpecString(String bucketSpec, BucketConfiguration defaultConfig) {
        List<BucketConfiguration> configs = new ArrayList<BucketConfiguration>();
        if (bucketSpec != null) {
            for (String spec : bucketSpec.split(",")) {
                BucketConfiguration config = new BucketConfiguration(defaultConfig);

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
                configs.add(config);
            }
        }

        if (configs.isEmpty()) {
            BucketConfiguration defaultBucket = new BucketConfiguration(defaultConfig);
            defaultBucket.name = "default";
            configs.add(defaultBucket);
        }
        return configs;
    }

    /**
     * Initializes the default configuration from the command line parameters. This is present in order to allow the
     * super constructor to be the first statement
     */
    private static BucketConfiguration createDefaultConfig(String hostname, int numNodes, int bucketStartPort, int numVBuckets, int numReplicas) {
        BucketConfiguration defaultConfig = new BucketConfiguration();
        defaultConfig.type = BucketType.COUCHBASE;
        defaultConfig.hostname = hostname;
        defaultConfig.numNodes = numNodes;
        if (numReplicas > -1) {
            defaultConfig.numReplicas = numReplicas;
        }

        defaultConfig.bucketStartPort = bucketStartPort;
        defaultConfig.numVBuckets = numVBuckets;
        return defaultConfig;
    }

    public CouchbaseMock(String hostname, int port, int numNodes, int bucketStartPort, int numVBuckets, String bucketSpec, int numReplicas) throws IOException {
        this(port, fromSpecString(bucketSpec, createDefaultConfig(hostname, numNodes, bucketStartPort, numVBuckets, numReplicas)));
        defaultConfig = createDefaultConfig(hostname, numNodes, bucketStartPort, numVBuckets, numReplicas);
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

    /**
     * Create a new CouchbaseMock object.
     * @param port The REST port which the mock should listen on. If set to 0, a random available
     *             port will be selected
     * @param configs A list of bucket configurations which the mock should start when the
     *                {@link #start()} method is called.
     * @throws IOException
     */
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
        httpServer.register("/query/*", new QueryServer());
    }

    /**
     * Wait until all initial buckets have been created
     * @throws InterruptedException
     */
    public void waitForStartup() throws InterruptedException {
        startupLatch.await();
    }

    /**
     * Get The port of the http server providing the REST interface.
     * @return The REST API port
     */
    public int getHttpPort() {
        return port;
    }

    /**
     * Get the name of the host to which the REST API is bound
     * @return The bound host
     */
    public String getHttpHost() {
        return "127.0.0.1";
    }

    /**
     * Get the authenticator object which can be used to verify credentials for access to the cluster
     * @return The authenticator
     */
    public Authenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * Create a new bucket, and start it.
     * @param config The bucket configuration to use
     * @throws BucketAlreadyExistsException If the bucket already exists
     * @throws IOException
     */
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

    /**
     * Destroy a bucket
     * @param name The name of the bucket to remove
     * @throws FileNotFoundException If the bucket does not exist
     */
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

    /**
     * Used for the command line, this ensures that the CountDownLatch object is only set to 0
     * when all the command line parameters have been initialized; so that when the monitor
     * finally sends the port over the socket, all the items will have already been initialized.
     * @param docsFile Document file to load
     * @param monitorAddress Monitor address
     * @param useBeerSample Whether to load the beer-sample bucket
     * @throws IOException
     */
    private void start(String docsFile, String monitorAddress, boolean useBeerSample) throws IOException {
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
            } catch (BucketAlreadyExistsException ex) {
                throw new IOException(ex);
            }
        }
        httpServer.start();

        // See if we need to load documents:
        if (docsFile != null) {
            DocumentLoader loader = new DocumentLoader(this, "default");
            loader.loadDocuments(docsFile);
        } else if (useBeerSample) {
            RestAPIUtil.loadBeerSample(this);
        }

        if (monitorAddress != null) {
            startHarakiriMonitor(monitorAddress, true);
        }

        startupLatch.countDown();
    }

    /**
     * Start the mock. This will open the REST API port and initialize any buckets
     * which are configured in the initial configuration list.
     *
     * To stop the cluster, invoke {@link #stop()}
     */
    public void start() throws IOException {
        start(null, null, false);
    }

    /**
     * Stops the cluster. This stops the server listening on the REST API port, and also destroys
     * any buckets which are part of the cluster.
     */
    public void stop() {
        httpServer.stopServer();
        for (Bucket bucket : buckets.values()) {
            bucket.stop();
        }
    }

    private static void printHelp() {
        final PrintStream o = System.out;
        BucketConfiguration defaultConfig = new BucketConfiguration();

        o.printf("Options are:%n");
        o.printf("-h --host             The hostname for the REST port. Default=8091%n");
        o.printf("-b --buckets          (See description below%n");
        o.printf("-p --nodes            The number of nodes each bucket should contain. Default=%d%n", defaultConfig.numNodes);
        o.printf("-v --vbuckets         The number of vbuckets each bucket should contain. Default=%d%n", defaultConfig.numVBuckets);
        o.printf("-R --replicas         The number of replica nodes for each bucket. Default=%d%n", defaultConfig.numReplicas);
        o.printf("   --harakiri-monitor The host:port on which the control socket should connect to%n");
        o.printf("-S --with-beer-sample Initialize the cluster with the `beer-sample` bucket active%n");
        o.printf("-D --docs             Specify a ZIP file that should contain documents to be loaded%n");
        o.printf("                      into the `default` bucket%n");
        o.printf("-E --empty            Initialize a blank cluster without any buckets. Buckets may then%n");
        o.printf("                      be later added via the REST API%n");
        o.printf("%n");
        o.printf("=== -- bucket option ===%n");
        o.printf("Buckets descriptions is a comma-separated list of {name}:{password}:{bucket type} pairs.%n");
        o.printf("To allow unauthorized connections, omit password.%n");
        o.printf("Third parameter could be either 'memcache' or 'couchbase' (default value is 'couchbase'). E.g.%n");
        o.printf("    default:,test:,protected:secret,cache::memcache%n");
        o.printf("The default is equivalent to `couchbase::`%n");
    }

    @SuppressWarnings("ConstantConditions")
    public static void main(String[] args) {
        BucketConfiguration defaultConfig = new BucketConfiguration();
        int port = 8091;
        int nodes = defaultConfig.numNodes;
        int vbuckets = defaultConfig.numVBuckets;
        int replicaCount = defaultConfig.numReplicas;

        String harakiriMonitorAddress = null;
        String hostname = null;
        String bucketsSpec = null;
        String docsFile = null;
        boolean useBeerSample = false;
        boolean emptyCluster = false;

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
                printHelp();
                System.exit(0);
            }
        }

        try {
            CouchbaseMock mock = new CouchbaseMock(hostname, port, nodes, 0, vbuckets, bucketsSpec, replicaCount);
            if (emptyCluster) {
                mock.clearInitialConfigs();
            }

            mock.start(docsFile, harakiriMonitorAddress, useBeerSample);

        } catch (Exception e) {
            Logger.getLogger(CouchbaseMock.class.getName()).log(Level.SEVERE, "Could not create cluster: ", e);
            System.exit(1);
        }
    }
}
