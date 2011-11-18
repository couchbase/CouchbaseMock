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
package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.ComCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.memcached.protocol.BinaryCommand;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.security.AccessControlException;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONObject;

/**
 * This is a small implementation of a Memcached server. It listens
 * to exactly one port and implements the binary protocol.
 *
 * @author Trond Norbye
 */
public class MemcachedServer implements Runnable, BinaryProtocolHandler {

    private final DataStore datastore;
    private final long bootTime;
    private final String hostname;
    private final ServerSocketChannel server;
    private Selector selector;
    private final int port;
    private CountDownLatch listenLatch;
    private CommandExecutor[] executors = new CommandExecutor[0xff];

    /**
     * Create a new new memcached server.
     *
     * @param hostname The hostname to bind to (null == any)
     * @param port The port this server should listen to (0 to choose an
     *             ephemeral port)
     * @param datastore
     * @throws IOException If we fail to create the server socket
     */
    public MemcachedServer(String hostname, int port, DataStore datastore) throws IOException {
        this.datastore = datastore;

        UnknownCommandExecutor unknownHandler = new UnknownCommandExecutor();
        for (int ii = 0; ii < executors.length; ++ii) {
            executors[ii] = unknownHandler;
        }

        executors[ComCode.ADD.cc()] = new StoreCommandExecutor();
        executors[ComCode.ADDQ.cc()] = executors[ComCode.ADD.cc()];
        executors[ComCode.SET.cc()] = executors[ComCode.ADD.cc()];
        executors[ComCode.SETQ.cc()] = executors[ComCode.ADD.cc()];
        executors[ComCode.REPLACE.cc()] = executors[ComCode.ADD.cc()];
        executors[ComCode.REPLACEQ.cc()] = executors[ComCode.ADD.cc()];
        executors[ComCode.DELETE.cc()] = new DeleteCommandExecutor();
        executors[ComCode.DELETEQ.cc()] = executors[ComCode.DELETE.cc()];
        executors[ComCode.GET.cc()] = new GetCommandExecutor();
        executors[ComCode.GETQ.cc()] = executors[ComCode.GET.cc()];
        executors[ComCode.GETK.cc()] = executors[ComCode.GET.cc()];
        executors[ComCode.GETKQ.cc()] = executors[ComCode.GET.cc()];
        executors[ComCode.INCREMENT.cc()] = new ArithmeticCommandExecutor();
        executors[ComCode.INCREMENTQ.cc()] = executors[ComCode.INCREMENT.cc()];
        executors[ComCode.DECREMENT.cc()] = executors[ComCode.INCREMENT.cc()];
        executors[ComCode.DECREMENTQ.cc()] = executors[ComCode.INCREMENT.cc()];

        bootTime = System.currentTimeMillis() / 1000;
        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        if (hostname != null && !hostname.equals("*")) {
            server.socket().bind(new InetSocketAddress(hostname, port));
            this.hostname = hostname;
        } else {
            server.socket().bind(new InetSocketAddress(port));
            InetAddress address = server.socket().getInetAddress();
            if (address.isAnyLocalAddress()) {
                String name;
                try {
                    name = InetAddress.getLocalHost().getHostAddress();
                } catch (UnknownHostException ex) {
                    name = "localhost";
                }
                this.hostname = name;
            } else {
                this.hostname = address.getHostName();
            }
        }
        this.port = server.socket().getLocalPort();
        this.listenLatch = new CountDownLatch(0);
    }

    public DataStore getDatastore() {
        return datastore;
    }

    @Override
    public String toString() {
        Map<String, Object> map = new HashMap<String, Object>();
        long now = System.currentTimeMillis() / 1000;
        int uptime = (int) (now - bootTime);
        map.put("uptime", new Long(uptime));
        map.put("replication", 1);
        map.put("clusterMembership", "active");
        map.put("status", "healthy");
        map.put("hostname", hostname);
        map.put("clusterCompatibility", 1);
        map.put("version", "9.9.9");
        StringBuilder sb = new StringBuilder(System.getProperty("os.arch"));
        sb.append("-");
        sb.append(System.getProperty("os.name"));
        sb.append("-");
        sb.append(System.getProperty("os.version"));
        map.put("os", sb.toString().replaceAll(" ", "_"));
        Map<String, Integer> ports = new HashMap<String, Integer>();
        ports.put("direct", port);
        ports.put("proxy", 0); //todo this should be fixed (Vitaly.R)
        map.put("ports", ports);
        return JSONObject.fromObject(map).toString();
    }

    public String getSocketName() {
        return hostname + ":" + port;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                listenLatch.await();
                if (selector == null || !selector.isOpen()) {
                    selector = Selector.open();
                    server.register(selector, SelectionKey.OP_ACCEPT);
                }
                if (selector.select() < 1) {
                    // don't even try call other methods on selector
                    // because it could be closed already by shutdown()
                    continue;
                }
                Set<SelectionKey> readyKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = readyKeys.iterator();

                // @todo we should probably drive the state machine until it
                // step doesn't do any progress to avoid jumping back to the
                // core
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    MemcachedConnection client = (MemcachedConnection) key.attachment();

                    if (client != null) {

                        try {

                            if (key.isReadable()) {
                                SocketChannel channel = (SocketChannel) key.channel();

                                if (channel.read(client.getInputBuffer()) == -1) {
                                    channel.close();
                                } else {
                                    client.step();
                                    if (client.hasOutput()) {
                                        channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, client);
                                    } else {
                                        channel.register(selector, SelectionKey.OP_READ, client);
                                    }
                                }
                            } else if (key.isWritable()) {
                                SocketChannel channel = (SocketChannel) key.channel();
                                channel.write(client.getOutputBuffer());
                            }
                        } catch (ClosedChannelException exp) {
                            // just ditch this client..
                        }
                    } else {
                        if (key.isAcceptable()) {
                            SocketChannel cc = server.accept();
                            cc.configureBlocking(false);
                            cc.register(selector, SelectionKey.OP_READ, new MemcachedConnection(this));
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getLocalizedMessage());
            } catch (InterruptedException ex) {
                System.err.println(ex.getLocalizedMessage());
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.membase.libmembase.test.BinaryProtocolHandler#execute(org.membase
     * .libmembase.test.BinaryCommand,
     * org.membase.libmembase.test.MemcachedConnection)
     */
    @Override
    public void execute(BinaryCommand cmd, MemcachedConnection client)
            throws IOException {
        try {
            executors[cmd.getComCode().cc()].execute(cmd, this, client);
        } catch (AccessControlException ex) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.NOT_MY_VBUCKET));
        }
    }

    BinaryProtocolHandler getProtocolHandler() {
        return this;
    }

    public void shutdown() {
        try {
            this.listenLatch = new CountDownLatch(1);
            this.selector.close();
        } catch (IOException ex) {
            Logger.getLogger(MemcachedServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void startup() {
        this.listenLatch.countDown();
    }

    /**
     * Program entry point that runs the memcached server as a standalone
     * server just like any other memcached server...
     *
     * @param args Program arguments (not used)
     */
    public static void main(String[] args) {
        try {
            DataStore ds = new DataStore(1024);
            MemcachedServer server = new MemcachedServer(null, 11211, ds);
            for (int ii = 0; ii < 1024; ++ii) {
                ds.setOwnership(ii, server);
            }
            server.run();
        } catch (IOException e) {
            Logger.getLogger(MemcachedServer.class.getName()).log(Level.SEVERE, "Fatal error! failed to create socket: ", e);
        }
    }
}
