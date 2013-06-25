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
import org.couchbase.mock.memcached.protocol.CommandCode;
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

    private final DataStore dataStore;
    private final long bootTime;
    private final String hostname;
    private final ServerSocketChannel server;
    private Selector selector;
    private final int port;
    private final CommandExecutor[] executors = new CommandExecutor[0xff];
    private final Bucket bucket;
    private boolean active = true;
    private int hiccupTime = 0;
    private int hiccupOffset = 0;
    private int truncateLimit = 0;

    /**
     * Create a new new memcached server.
     *
     * @param bucket    The bucket owning all of the stores
     * @param hostname  The hostname to bind to (null == any)
     * @param port      The port this server should listen to (0 to choose an
     *                  ephemeral port)
     * @param dataStore The store used to keep the data
     * @throws IOException If we fail to create the server socket
     */
    public MemcachedServer(Bucket bucket, String hostname, int port, DataStore dataStore) throws IOException {
        this.bucket = bucket;
        this.dataStore = dataStore;

        UnknownCommandExecutor unknownHandler = new UnknownCommandExecutor();
        for (int ii = 0; ii < executors.length; ++ii) {
            executors[ii] = unknownHandler;
        }

        executors[CommandCode.QUIT.cc()] = new QuitCommandExecutor();
        executors[CommandCode.QUITQ.cc()] = new QuitCommandExecutor();
        executors[CommandCode.FLUSH.cc()] = new FlushCommandExecutor();
        executors[CommandCode.FLUSHQ.cc()] = new FlushCommandExecutor();
        executors[CommandCode.NOOP.cc()] = new NoopCommandExecutor();
        executors[CommandCode.VERSION.cc()] = new VersionCommandExecutor();
        executors[CommandCode.STAT.cc()] = new StatCommandExecutor();
        executors[CommandCode.VERBOSITY.cc()] = new VerbosityCommandExecutor();
        executors[CommandCode.ADD.cc()] = new StoreCommandExecutor();
        executors[CommandCode.ADDQ.cc()] = executors[CommandCode.ADD.cc()];
        executors[CommandCode.APPEND.cc()] = new AppendCommandExecutor();
        executors[CommandCode.APPENDQ.cc()] = new AppendCommandExecutor();
        executors[CommandCode.PREPEND.cc()] = new PrependCommandExecutor();
        executors[CommandCode.PREPENDQ.cc()] = new PrependCommandExecutor();
        executors[CommandCode.SET.cc()] = executors[CommandCode.ADD.cc()];
        executors[CommandCode.SETQ.cc()] = executors[CommandCode.ADD.cc()];
        executors[CommandCode.REPLACE.cc()] = executors[CommandCode.ADD.cc()];
        executors[CommandCode.REPLACEQ.cc()] = executors[CommandCode.ADD.cc()];
        executors[CommandCode.DELETE.cc()] = new DeleteCommandExecutor();
        executors[CommandCode.DELETEQ.cc()] = executors[CommandCode.DELETE.cc()];
        executors[CommandCode.GET.cc()] = new GetCommandExecutor();
        executors[CommandCode.GETQ.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GETK.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GETKQ.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GETL.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.TOUCH.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GAT.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GATQ.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.UNL.cc()] = new UnlockCommnandExecutor();
        executors[CommandCode.INCREMENT.cc()] = new ArithmeticCommandExecutor();
        executors[CommandCode.INCREMENTQ.cc()] = executors[CommandCode.INCREMENT.cc()];
        executors[CommandCode.DECREMENT.cc()] = executors[CommandCode.INCREMENT.cc()];
        executors[CommandCode.DECREMENTQ.cc()] = executors[CommandCode.INCREMENT.cc()];
        executors[CommandCode.SASL_LIST_MECHS.cc()] = new SaslCommandExecutor();
        executors[CommandCode.SASL_AUTH.cc()] = executors[CommandCode.SASL_LIST_MECHS.cc()];
        executors[CommandCode.SASL_STEP.cc()] = executors[CommandCode.SASL_LIST_MECHS.cc()];

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

    public DataStore getDataStore() {
        return dataStore;
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public String toString() {
        Map<String, Object> map = new HashMap<String, Object>();
        long now = System.currentTimeMillis() / 1000;
        long uptime = now - bootTime;
        map.put("uptime", uptime);
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

    @SuppressWarnings("SpellCheckingInspection")
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

    private void writeResponse(SocketChannel channel, ByteBuffer buf)
            throws IOException {
        int wv;
        int nw = 0;

        do {
            wv = channel.write(buf);
            nw += wv;
        } while (wv > 0);
        if (wv == -1) {
            channel.close();
            throw new ClosedChannelException();
        }
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

                        handleClient(key);
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

    private void handleClient(SelectionKey key) throws IOException {
        MemcachedConnection client = (MemcachedConnection) key.attachment();

        if (client != null) {
            try {

                int ioEvents = SelectionKey.OP_READ;
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
                        if (truncateLimit > 0 && buf.limit() > truncateLimit) {
                            buf.limit(truncateLimit);
                        }

                        if (hiccupOffset > 0 && buf.limit() > hiccupOffset) {
                            ByteBuffer immediateBuf = buf.slice();
                            buf.position(hiccupOffset);
                            immediateBuf.limit(hiccupOffset);
                            writeResponse(channel, immediateBuf);

                            // Wait hiccupTime to write the rest of the buffer
                            //noinspection EmptyCatchBlock
                            try {
                                Thread.sleep(hiccupTime);
                            } catch (InterruptedException ex) {
                            }
                        }
                        writeResponse(channel, buf);
                    }
                }

                if (client.hasOutput()) {
                    ioEvents |= SelectionKey.OP_WRITE;
                }

                channel.register(selector, ioEvents, client);
            } catch (ClosedChannelException exp) {
                // just ditch this client..
            } catch (IOException ex) {
                // hmm.. should this really be silently ignored?
            }

        } else {
            if (key.isAcceptable()) {
                SocketChannel cc = server.accept();
                cc.configureBlocking(false);
                cc.register(selector, SelectionKey.OP_READ, new MemcachedConnection(this));
            }
        }
    }

    public Bucket getBucket() {
        return bucket;
    }

    @Override
    public void execute(BinaryCommand cmd, MemcachedConnection client)
            throws IOException {
        try {
            if (client.isAuthenticated()
                    || cmd.getComCode() == CommandCode.SASL_AUTH
                    || cmd.getComCode() == CommandCode.SASL_LIST_MECHS
                    || cmd.getComCode() == CommandCode.SASL_STEP) {
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
     * @param milliSeconds how long to stall for
     * @param offset how far along the output buffer should we hiccup
     */
    public void setHiccup(int milliSeconds, int offset) {
        if (milliSeconds < 0 || offset < 0) {
            throw new IllegalArgumentException("Time and offset must be >= 0");
        }

        hiccupTime = milliSeconds;
        hiccupOffset = offset;
    }

    public void setTruncateLimit(int limit) {
        truncateLimit = limit;
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
