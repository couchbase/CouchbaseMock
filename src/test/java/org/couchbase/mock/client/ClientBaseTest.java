/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.client;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.vbucket.VBucketNodeLocator;
import junit.framework.TestCase;
import net.spy.memcached.MemcachedNode;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.memcached.MemcachedServer;
import org.couchbase.mock.memcached.VBucketInfo;
import org.couchbase.mock.memcached.client.MemcachedClient;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base test case which uses a client.
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public abstract class ClientBaseTest extends TestCase {
    protected final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    protected MockClient mockClient;
    protected CouchbaseMock couchbaseMock;

    protected final CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    protected CouchbaseClient client;
    protected CouchbaseConnectionFactory connectionFactory;

    public ClientBaseTest() {}

    protected MemcachedNode getMasterForKey(String key) {
        VBucketNodeLocator locator = (VBucketNodeLocator) client.getNodeLocator();
        return locator.getPrimary(key);
    }

    // Don't make the client flood the screen with log messages..
    static public void initJCBCEnv() {
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
        System.setProperty("cbclient.disableCarrierBootstrap", "true");
        Logger.getLogger("net.spy.memcached").setLevel(Level.WARNING);
        Logger.getLogger("com.couchbase.client").setLevel(Level.WARNING);
        Logger.getLogger("com.couchbase.client.vbucket").setLevel(Level.WARNING);
    }

    static {
        initJCBCEnv();
    }

    protected void createMock(@NotNull String name, @NotNull String password) throws Exception {
        bucketConfiguration.numNodes = 10;
        bucketConfiguration.numReplicas = 3;
        bucketConfiguration.name = name;
        bucketConfiguration.type = BucketType.COUCHBASE;
        bucketConfiguration.password = password;
        ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        couchbaseMock = new CouchbaseMock(0, configList);
        couchbaseMock.start();
        couchbaseMock.waitForStartup();

    }

    protected void createClients() throws Exception {
        mockClient = new MockClient(new InetSocketAddress("localhost", 0));
        couchbaseMock.startHarakiriMonitor("localhost:" + mockClient.getPort(), false);
        mockClient.negotiate();

        List<URI> uriList = new ArrayList<URI>();
        uriList.add(new URI("http", null, "localhost", couchbaseMock.getHttpPort(), "/pools", "", ""));
        connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
        client = new CouchbaseClient(connectionFactory);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        createMock("default", "");
        createClients();
    }

    @Override
    protected void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
        if (couchbaseMock != null) {
            couchbaseMock.stop();
        }
        if (mockClient != null) {
            mockClient.shutdown();
        }
        super.tearDown();
    }


    protected MemcachedClient getBinClient(int index) throws IOException {
        MemcachedServer server = getServer(index);
        Socket sock = new Socket();
        sock.connect(new InetSocketAddress(server.getHostname(), server.getPort()));
        return new MemcachedClient(sock);
    }

    private Bucket getBucket() {
        return couchbaseMock.getBuckets().get(bucketConfiguration.name);
    }

    protected MemcachedClient getBinClient() throws IOException {
        return getBinClient(0);
    }

    protected MemcachedServer getServer(int index) {
        return getBucket().getServers()[index];
    }

    /**
     * Gets a valid vBucket ID for a given server. Used to generate a packet that
     * will be accepted by it.
     * @param on The index of the server
     * @return The vBucket
     */
    protected short findValidVbucket(int on) {
        Bucket bucket = getBucket();
        VBucketInfo[] vbi = bucket.getVBucketInfo();
        MemcachedServer target = getServer(on);
        for (int i = 0; i < vbi.length; i++) {
            VBucketInfo cur = vbi[i];
            if (cur.getOwner().equals(target)) {
                return (short)i;
            }
        }
        return -1;
    }
}
