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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import junit.framework.TestCase;
import net.spy.memcached.MemcachedNode;
import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;
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
    static {
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
        Logger.getLogger("net.spy.memcached").setLevel(Level.WARNING);
        Logger.getLogger("com.couchbase.client").setLevel(Level.WARNING);
        Logger.getLogger("com.couchbase.client.vbucket").setLevel(Level.WARNING);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        bucketConfiguration.numNodes = 10;
        bucketConfiguration.numReplicas = 3;
        bucketConfiguration.name = "default";
        bucketConfiguration.type = BucketType.COUCHBASE;
        ArrayList configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        couchbaseMock = new CouchbaseMock(0, configList);
        couchbaseMock.start();
        couchbaseMock.waitForStartup();

        mockClient = new MockClient(new InetSocketAddress("localhost", 0));
        couchbaseMock.setupHarakiriMonitor("localhost:" + mockClient.getPort(), false);
        mockClient.negotiate();

        List<URI> uriList = new ArrayList<URI>();
        uriList.add(new URI("http", null, "localhost", couchbaseMock.getHttpPort(), "/pools", "", ""));
        connectionFactory = cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);
        client = new CouchbaseClient(connectionFactory);
    }

    @Override
    protected void tearDown() throws Exception {
        client.shutdown();
        couchbaseMock.stop();
        mockClient.shutdown();
        super.tearDown();
    }
}
