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

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.security.AccessControlException;

import java.util.concurrent.CountDownLatch;
import java.util.Iterator;
import java.util.Set;

import org.couchbase.mock.util.JSON;

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

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{");

        long now = System.currentTimeMillis() / 1000;
        int uptime = (int) (now - bootTime);
        JSON.addElement(pw, "uptime", uptime, true);
        JSON.addElement(pw, "replication", 1, true);
        JSON.addElement(pw, "clusterMembership", "active", true);
        JSON.addElement(pw, "status", "healthy", true);
        JSON.addElement(pw, "hostname", hostname, true);
        JSON.addElement(pw, "clusterCompatibility", 1, true);
        JSON.addElement(pw, "version", "9.9.9", true);
        StringBuilder sb = new StringBuilder(System.getProperty("os.arch"));
        sb.append("-");
        sb.append(System.getProperty("os.name"));
        sb.append("-");
        sb.append(System.getProperty("os.version"));
        JSON.addElement(pw, "os", sb.toString().replaceAll(" ", "_"), true);
        pw.print("\"ports\":{");
        JSON.addElement(pw, "direct", port, true);
        JSON.addElement(pw, "proxy", 0, false); //todo this should be fixed (Vitaly.R)
        pw.print("}}");
        pw.flush();
        return sw.toString();
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
                selector.select();
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
                        assert key.isAcceptable();

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
        BinaryResponse res = null;
        ErrorCode err;
        Item item;
        try {
            switch (cmd.getComCode()) {
                case ADD:
                case ADDQ:
                    item = cmd.getItem();
                    err = datastore.add(this, cmd.getVBucketId(), item);
                    res = new BinaryResponse(cmd, err, item.getCas());
                    break;
                case REPLACE:
                case REPLACEQ:
                    item = cmd.getItem();
                    err = datastore.replace(this, cmd.getVBucketId(), item);
                    res = new BinaryResponse(cmd, err, item.getCas());
                    break;
                case SET:
                case SETQ:
                    item = cmd.getItem();
                    err = datastore.set(this, cmd.getVBucketId(), item);
                    res = new BinaryResponse(cmd, err, item.getCas());
                    break;

                case DELETE:
                case DELETEQ:
                    err = datastore.delete(this, cmd.getVBucketId(),
                            cmd.getKey(), cmd.getCas());
                    res = new BinaryResponse(cmd, err);
                    break;

                case STAT:
                    res = new BinaryResponse(cmd, ErrorCode.SUCCESS);
                    break;

                case NOOP:
                    res = new BinaryResponse(cmd, ErrorCode.SUCCESS);
                    break;

                case GET:
                case GETQ:
                case GETK:
                case GETKQ:
                    item = datastore.get(this, cmd.getVBucketId(), cmd.getKey());
                    if (item == null) {
                        res = new BinaryResponse(cmd, ErrorCode.KEY_ENOENT);
                    } else {
                        res = new BinaryResponse(cmd, item);
                    }
                    break;

                case APPEND:
                case APPENDQ:
                case PREPEND:
                case PREPENDQ:
                case VERSION:
                case FLUSH:
                case FLUSHQ:
                case QUIT:
                case QUITQ:
                case INCREMENT:
                case INCREMENTQ:
                case DECREMENT:
                case DECREMENTQ:

                default:
                    res = new BinaryResponse(cmd, ErrorCode.UNKNOWN_COMMAND);
            }
        } catch (AccessControlException ex) {
            res = new BinaryResponse(cmd, ErrorCode.NOT_MY_VBUCKET);
        }

        if (res != null) {
            switch (res.getComCode()) {
                case GETQ:
                case GETKQ:
                    if (res.getErrorCode() == ErrorCode.KEY_ENOENT) {
                        res = null;
                    }
                    break;
                case ADDQ:
                case SETQ:
                case REPLACEQ:
                    if (res.getErrorCode() == ErrorCode.SUCCESS) {
                        res = null;
                    }
                    break;
                default:
                    ;
            }

            if (res != null) {
                client.sendResponse(res);
            }
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
            System.err.print("Fatal error! failed to create socket: ");
            System.err.println(e.getLocalizedMessage());
        }
    }
}
