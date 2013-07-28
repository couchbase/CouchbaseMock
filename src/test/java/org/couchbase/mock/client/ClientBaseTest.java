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
    protected final BucketConfiguration bconf = new BucketConfiguration();
    protected BufferedReader harakiriInput;
    protected OutputStream harakiriOutput;
    private Socket harakiriConnection;

    public ClientBaseTest() {}

    protected final CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    protected CouchbaseClient client;
    protected CouchbaseConnectionFactory connectionFactory;
    protected CouchbaseMock mock;

    protected MemcachedNode getMasterForKey(String key) {
        VBucketNodeLocator locator = (VBucketNodeLocator) client.getNodeLocator();
        MemcachedNode master = locator.getPrimary(key);
        return master;
    }


    private interface Listener extends Runnable {
        public Socket getClientSocket();
    }

    private void setupHarakiri() throws Exception {
        final ServerSocket sock = new ServerSocket(0);
        final int port = sock.getLocalPort();

        Listener listener = new Listener() {
            public Socket clientSocket = null;

            @Override public Socket getClientSocket() {
                return clientSocket;
            }

            @Override public void run() {
                try {
                    clientSocket = sock.accept();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        Thread listenThread = new Thread(listener);
        String hostString = sock.getInetAddress().getHostAddress() + ":" + port;

        mock.setupHarakiriMonitor(hostString, false);
        listenThread.start();
        listenThread.join();
        harakiriConnection = listener.getClientSocket();
        if (harakiriConnection == null) {
            throw new IOException("Couldn't get client socket");
        }

        harakiriInput = new BufferedReader(
                new InputStreamReader(harakiriConnection.getInputStream()));
        harakiriOutput = harakiriConnection.getOutputStream();

        /**
         * We need to negotiate the port now.
         */
        StringBuilder sb = new StringBuilder();
        char c;
        while ( (c = (char)harakiriInput.read()) != '\0' ) {
            sb.append(c);
        }
        assertEquals(Integer.parseInt(sb.toString()), mock.getHttpPort());
    }

    // Don't make the client flood the screen with log messages..
    static {
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
        Logger.getLogger("net.spy.memcached").setLevel(Level.WARNING);
        Logger.getLogger("com.couchbase.client").setLevel(Level.WARNING);
        Logger.getLogger("com.couchbase.client.vbucket").setLevel(Level.WARNING);
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        bconf.numNodes = 10;
        bconf.numReplicas = 2;
        bconf.name = "default";
        bconf.type = BucketType.COUCHBASE;
        ArrayList configList = new ArrayList<BucketConfiguration>();
        configList.add(bconf);
        mock = new CouchbaseMock(0, configList);
        mock.start();
        mock.waitForStartup();
        setupHarakiri();

        List<URI> uriList = new ArrayList<URI>();
        uriList.add(new URI("http", null, "localhost", mock.getHttpPort(), "/pools", "", ""));
        connectionFactory = cfb.buildCouchbaseConnection(uriList, bconf.name, bconf.password);
        client = new CouchbaseClient(connectionFactory);
    }

    @Override
    protected void tearDown() throws Exception {
        client.shutdown();
        mock.stop();
        super.tearDown();
    }
}
