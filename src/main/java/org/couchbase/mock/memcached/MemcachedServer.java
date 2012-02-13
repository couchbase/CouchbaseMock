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

import java.nio.ByteBuffer;
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

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONObject;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.Bucket.BucketType;

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
    private CommandExecutor[] executors = new CommandExecutor[0xff];
    private final Bucket bucket;
    private boolean active = true;

    /**
     * Create a new new memcached server.
     *
     * @param hostname The hostname to bind to (null == any)
     * @param port The port this server should listen to (0 to choose an
     *             ephemeral port)
     * @param datastore
     * @param bucket
     * @throws IOException If we fail to create the server socket
     */
    public MemcachedServer(Bucket bucket, String hostname, int port, DataStore datastore) throws IOException {
        this.bucket = bucket;
        this.datastore = datastore;

        UnknownCommandExecutor unknownHandler = new UnknownCommandExecutor();
        for (int ii = 0; ii < executors.length; ++ii) {
            executors[ii] = unknownHandler;
        }

        executors[ComCode.QUIT.cc()] = new QuitCommandExecutor();
        executors[ComCode.QUITQ.cc()] = new QuitCommandExecutor();
        executors[ComCode.FLUSH.cc()] = new FlushCommandExecutor();
        executors[ComCode.FLUSHQ.cc()] = new FlushCommandExecutor();
        executors[ComCode.NOOP.cc()] = new NoopCommandExecutor();
        executors[ComCode.VERSION.cc()] = new VersionCommandExecutor();
        executors[ComCode.STAT.cc()] = new StatCommandExecutor();
        executors[ComCode.VERBOSITY.cc()] = new VerbosityCommandExecutor();
        executors[ComCode.ADD.cc()] = new StoreCommandExecutor();
        executors[ComCode.ADDQ.cc()] = executors[ComCode.ADD.cc()];
        executors[ComCode.APPEND.cc()] = new AppendCommandExecutor();
        executors[ComCode.APPENDQ.cc()] = new AppendCommandExecutor();
        executors[ComCode.PREPEND.cc()] = new PrependCommandExecutor();
        executors[ComCode.PREPENDQ.cc()] = new PrependCommandExecutor();
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
        executors[ComCode.TOUCH.cc()] = executors[ComCode.GET.cc()];
        executors[ComCode.GAT.cc()] = executors[ComCode.GET.cc()];
        executors[ComCode.GATQ.cc()] = executors[ComCode.GET.cc()];
        executors[ComCode.INCREMENT.cc()] = new ArithmeticCommandExecutor();
        executors[ComCode.INCREMENTQ.cc()] = executors[ComCode.INCREMENT.cc()];
        executors[ComCode.DECREMENT.cc()] = executors[ComCode.INCREMENT.cc()];
        executors[ComCode.DECREMENTQ.cc()] = executors[ComCode.INCREMENT.cc()];
        executors[ComCode.SASL_LIST_MECHS.cc()] = new SaslCommandExecutor();
        executors[ComCode.SASL_AUTH.cc()] = executors[ComCode.SASL_LIST_MECHS.cc()];
        executors[ComCode.SASL_STEP.cc()] = executors[ComCode.SASL_LIST_MECHS.cc()];

        bootTime = System.currentTimeMillis() / 1000;
        selector = Selector.open();
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
        server.register(selector, SelectionKey.OP_ACCEPT);
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
        map.put("hostname", getSocketName());
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

    public Map<String, String> getStats() {
        HashMap<String, String> stats = new HashMap<String, String>();
        stats.put("pid", Long.toString(Thread.currentThread().getId()));
        stats.put("time", Long.toString(new Date().getTime()));
        stats.put("version", "9.9.9");
        stats.put("uptime", "15554");
        stats.put("accepting_conns", "1");
        stats.put("auth_cmds", "0");
        stats.put("auth_errors", "0");
        stats.put("bucket_active_conns", "1");
        stats.put("bucket_conns", "3");
        stats.put("bytes_read", "1108621");
        stats.put("bytes_written", "205374436");
        stats.put("cas_badval", "0");
        stats.put("cas_hits", "0");
        stats.put("cas_misses", "0");
        return stats;
    }

    public String getSocketName() {
        return hostname + ":" + port;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    selector.select();
                    if (!active) {
                        // server is suspended: ignore all events
                        selector.selectedKeys().clear();
                        continue;
                    }
                } catch (IOException ex) {
                    continue;
                }

                try {
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

                    // @todo we should probably drive the state machine until it
                    // step doesn't do any progress to avoid jumping back to the
                    // core
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        iterator.remove();

                        MemcachedConnection client = (MemcachedConnection) key.attachment();

                        if (client != null) {
                            try {

                                int ioevents = SelectionKey.OP_READ;
                                SocketChannel channel = (SocketChannel) key.channel();

                                if (key.isReadable()) {
                                    if (channel.read(client.getInputBuffer()) == -1) {
                                        channel.close();
                                        throw new ClosedChannelException();
                                    } else {
                                        client.step();
                                    }
                                }
                                if (key.isWritable()) {
                                    ByteBuffer buf;
                                    while ((buf = client.getOutputBuffer()) != null) {
                                        if (channel.write(buf) == -1) {
                                            channel.close();
                                            throw new ClosedChannelException();
                                        }
                                    }
                                }

                                if (client.hasOutput()) {
                                    ioevents |= SelectionKey.OP_WRITE;
                                }

                                channel.register(selector, ioevents, client);
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
                    Logger.getLogger(MemcachedServer.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        } finally {
            try {
                server.close();
                selector.close();
            } catch (IOException e) {
                Logger.getLogger(MemcachedServer.class.getName()).log(Level.SEVERE, null, e);
            }
        }
    }

    public Bucket getBucket()
    {
        return bucket;
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
            if (client.isAuthenticated()
                    || cmd.getComCode() == ComCode.SASL_AUTH
                    || cmd.getComCode() == ComCode.SASL_LIST_MECHS
                    || cmd.getComCode() == ComCode.SASL_STEP) {
                executors[cmd.getComCode().cc()].execute(cmd, this, client);
            } else {
                client.sendResponse(new BinaryResponse(cmd, ErrorCode.AUTH_ERROR));
            }
        } catch (AccessControlException ex) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.NOT_MY_VBUCKET));
        }
    }

    BinaryProtocolHandler getProtocolHandler() {
        return this;
    }

    public void shutdown() {
        active = false;
    }

    public void startup() {
        active = true;
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
            MemcachedServer server = new MemcachedServer(null, null, 11211, ds);
            for (int ii = 0; ii < 1024; ++ii) {
                ds.setOwnership(ii, server);
            }
            server.run();
        } catch (IOException e) {
            Logger.getLogger(MemcachedServer.class.getName()).log(Level.SEVERE, "Fatal error! failed to create socket: ", e);
        }
    }

    /**
     * @return the active
     */
    public boolean isActive() {
        return active;
    }

    /**
     * @return the type
     */
    public BucketType getType() {
        return bucket.getType();
    }
}
