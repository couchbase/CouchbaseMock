/*
 * Copyright 2017 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.couchbase.mock.memcached;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.Bucket.BucketType;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.Info;
import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryConfigResponse;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.ErrorCode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.couchbase.mock.memcached.protocol.BinaryResponse.MAGIC;

/**
 * This is a small implementation of a Memcached server. It listens
 * to exactly one port and implements the binary protocol.
 *
 * @author Trond Norbye
 */
public class MemcachedServer extends Thread implements BinaryProtocolHandler {
    private final Storage storage;
    private final long bootTime;
    private final String hostname;
    private final ServerSocketChannel server;
    private Selector selector;
    private final int port;
    private final CommandExecutor[] executors = new CommandExecutor[0xff];
    private static final CommandExecutor unknownHandler = new UnknownCommandExecutor();
    private final Bucket bucket;
    private boolean active = true;
    private int hiccupTime = 0;
    private int hiccupOffset = 0;
    private int truncateLimit = 0;
    private boolean cccpEnabled = false;
    private final List<CommandLogEntry> commandLog = new ArrayList<CommandLogEntry>();
    private boolean shouldLogCommands = false;
    private boolean enhancedErrorsEnabled = false;
    private CompressionMode compression = CompressionMode.DISABLED;
    private List<String> saslMechanisms;

    public void setEnhancedErrorsEnabled(boolean enhancedErrorsEnabled) {
        this.enhancedErrorsEnabled = enhancedErrorsEnabled;
    }

    public boolean isEnhancedErrorsEnabled() {
        return enhancedErrorsEnabled;
    }

    public void setCompression(CompressionMode compression) {
        this.compression = compression;
    }

    public CompressionMode getCompression() {
        return compression;
    }

    public void setSaslMechanisms(List<String> saslMechanisms) {
        this.saslMechanisms = saslMechanisms;
    }

    public List<String> getSaslMechanisms() {
        return saslMechanisms;
    }

    public boolean supportsSaslMechanism(String mechanism) {
        return saslMechanisms.indexOf(mechanism) >= 0;
    }

    public static class CommandLogEntry {
        private final int opcode;
        private final long timestamp;
        CommandLogEntry(int opcode) {
            this.opcode = opcode;
            this.timestamp = System.currentTimeMillis();
        }
        public CommandLogEntry(int opcode, long timestamp) {
            this.opcode = opcode;
            this.timestamp = timestamp;
        }
        public long getMsTimestamp() {
            return timestamp;
        }
        public int getOpcode() {
            return opcode;
        }
    }

    public class FailMaker {
        private ErrorCode code = ErrorCode.SUCCESS;
        private int remaining = 0;

        public void update(ErrorCode code, int count) {
            this.code = code;
            this.remaining = count;
        }

        public ErrorCode getFailCode() {
            if (this.remaining == 0) {
                return ErrorCode.SUCCESS;
            }
            if (this.remaining > 0) {
                this.remaining--;
            }
            return code;
        }
    }

    private FailMaker failmaker = new FailMaker();

    /**
     * Create a new new memcached server.
     *
     * @param bucket    The bucket owning all of the stores
     * @param hostname  The hostname to connect to (null == any)
     * @param port      The port this server should listen to (0 to choose an
     *                  ephemeral port)
     * @param vbi       Vbucket Info
     * @throws IOException If we fail to create the server socket
     */
    public MemcachedServer(Bucket bucket, String hostname, int port, VBucketInfo[] vbi, boolean cccpEnabled) throws IOException {
        this.bucket = bucket;
        this.storage = new Storage(vbi, this);
        this.cccpEnabled = cccpEnabled;
        this.saslMechanisms = new ArrayList<String>();
        saslMechanisms.add("PLAIN"); /* only PLAIN should be supported by default */

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
        executors[CommandCode.APPEND.cc()] = new AppendPrependCommandExecutor();
        executors[CommandCode.APPENDQ.cc()] = new AppendPrependCommandExecutor();
        executors[CommandCode.PREPEND.cc()] = new AppendPrependCommandExecutor();
        executors[CommandCode.PREPENDQ.cc()] = new AppendPrependCommandExecutor();
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
        executors[CommandCode.TOUCH.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GAT.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.GATQ.cc()] = executors[CommandCode.GET.cc()];
        executors[CommandCode.INCREMENT.cc()] = new ArithmeticCommandExecutor();
        executors[CommandCode.INCREMENTQ.cc()] = executors[CommandCode.INCREMENT.cc()];
        executors[CommandCode.DECREMENT.cc()] = executors[CommandCode.INCREMENT.cc()];
        executors[CommandCode.DECREMENTQ.cc()] = executors[CommandCode.INCREMENT.cc()];
        executors[CommandCode.SASL_LIST_MECHS.cc()] = new SaslCommandExecutor();
        executors[CommandCode.SASL_AUTH.cc()] = executors[CommandCode.SASL_LIST_MECHS.cc()];
        executors[CommandCode.SASL_STEP.cc()] = executors[CommandCode.SASL_LIST_MECHS.cc()];
        executors[CommandCode.EVICT.cc()] = new EvictCommandExecutor();
        executors[CommandCode.HELLO.cc()] = new HelloCommandExecutor();
        executors[CommandCode.SELECT_BUCKET.cc()] = new SelectBucketCommandExecutor();

        // Sub-Document
        executors[CommandCode.SUBDOC_GET.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_EXISTS.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_DICT_ADD.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_DICT_UPSERT.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_DELETE.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_REPLACE.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_ARRAY_PUSH_LAST.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_ARRAY_PUSH_FIRST.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_ARRAY_ADD_UNIQUE.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_ARRAY_INSERT.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_COUNTER.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_GET_COUNT.cc()] = new SubdocCommandExecutor();
        executors[CommandCode.SUBDOC_MULTI_MUTATION.cc()] = new SubdocMultiCommandExecutor();
        executors[CommandCode.SUBDOC_MULTI_LOOKUP.cc()] = new SubdocMultiCommandExecutor();
        executors[CommandCode.GET_ERRMAP.cc()] = new GetErrmapCommandExecutor();

        // Couchbase buckets only
        if (bucket.getType() == BucketType.COUCHBASE) {
            executors[CommandCode.GETL.cc()] = executors[CommandCode.GET.cc()];
            executors[CommandCode.UNL.cc()] = new UnlockCommandExecutor();
            executors[CommandCode.GET_CLUSTER_CONFIG.cc()] = new ConfigCommandExecutor();
            executors[CommandCode.GET_REPLICA.cc()] = executors[CommandCode.GET.cc()];
            executors[CommandCode.OBSERVE.cc()] = new ObserveCommandExecutor();
            executors[CommandCode.OBSERVE_SEQNO.cc()] = new ObserveSeqnoCommandExecutor();
            executors[CommandCode.GET_RANDOM.cc()] = new GetRandomCommandExecutor();
        }

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

    public Storage getStorage() {
        return storage;
    }

    public void updateFailMakerContext(ErrorCode code, int count) {
        failmaker.update(code, count);
    }

    @SuppressWarnings("SpellCheckingInspection")
    public Map<String,Object> toNodeConfigInfo() {
        Map<String, Object> map = new HashMap<String, Object>();
        CouchbaseMock mock = bucket.getCluster();

        map.put("uptime", Long.toString(System.currentTimeMillis() - bootTime));
        map.put("replication", 1);
        map.put("clusterMembership", "active");
        map.put("status", "healthy");
        map.put("hostname", hostname + ":" + (mock == null ? "0" : mock.getHttpPort()));
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
        return map;
    }

    @SuppressWarnings("SpellCheckingInspection")
    private Map<String,String> getDefaultStats() {
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
        stats.put("mem_used", "100000000000000000000");
        stats.put("curr_connections", "-1");
        return stats;
    }

    @SuppressWarnings("SpellCheckingInspection")
    public Map<String, String> getStats(String about) {
        if (about == null || about.isEmpty()) {
            return getDefaultStats();
        } else if (about.equals("memory")) {
            Map<String, String> memStats = new HashMap<String, String>();
            Runtime rt = Runtime.getRuntime();
            memStats.put("mem_used", Long.toString(rt.totalMemory()));
            memStats.put("mem_free", Long.toString(rt.freeMemory()));
            memStats.put("mem_max", Long.toString(rt.maxMemory()));
            return memStats;
        } else if (about.equals("tap")) {
            Map<String, String> tapStats = new HashMap<String, String>();
            tapStats.put("ep_tap_count", "0");
            return tapStats;
        } else if (about.equals("__MOCK__")) {
            Map<String,String> mockInfo = new HashMap<String, String>();
            mockInfo.put("implementation", "java");
            mockInfo.put("version", Info.getVersion());
            return mockInfo;
        } else {
            return null;
        }
    }

    public String getSocketName() {
        return hostname + ":" + port;
    }

    public int getPort() {
        return port;
    }

    public String getHostname() {
        return hostname;
    }

    private void writeResponse(SocketChannel channel, OutputContext ctx) throws IOException {
        while (ctx.hasRemaining()) {
            ByteBuffer[] bufs = ctx.getIov();
            long nw = channel.write(bufs);
            if (nw < 0) {
                channel.close();
                throw new ClosedChannelException();
            } else if (nw == 0) {
                return;
            }
            ctx.updateBytesSent(nw);
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

    private void handleClientWrite(SocketChannel channel, OutputContext ctx) throws IOException {
        OutputContext effectiveCtx = ctx;
        if (truncateLimit > 0) {
            effectiveCtx = ctx.getSlice(truncateLimit);
        } else if (hiccupOffset > 0) {
            effectiveCtx = ctx.getSlice(hiccupOffset);
        }

        writeResponse(channel, effectiveCtx);
        if (hiccupOffset > 0) {
            try {
                Thread.sleep(hiccupTime);
            } catch (InterruptedException ex) {
            }
            writeResponse(channel, ctx);
        }
    }


    private void handleClientRead(SocketChannel channel, MemcachedConnection client) throws IOException {
        if (channel.read(client.getInputBuffer()) == -1) {
            channel.close();
            throw new ClosedChannelException();
        } else {
            client.step();
        }
    }

    private void handleNewClient() throws IOException {
        SocketChannel cc = server.accept();
        cc.configureBlocking(false);
        cc.socket().setTcpNoDelay(false);
        cc.socket().setSendBufferSize(1<<20);
        cc.socket().setReceiveBufferSize(1<<20);
        cc.register(selector, SelectionKey.OP_READ, new MemcachedConnection(this));
    }

    private void handleClient(SelectionKey key) throws IOException {
        MemcachedConnection client = (MemcachedConnection) key.attachment();
        if (client == null) {
            handleNewClient();
            return;
        }

        SocketChannel channel = (SocketChannel) key.channel();
        try {
            if (key.isReadable()) {
                handleClientRead(channel, client);
            }

            if (key.isWritable()) {
                OutputContext ctx = client.borrowOutputContext();

                if (ctx != null) {
                    try {
                        handleClientWrite(channel, ctx);
                    } finally {
                        client.returnOutputContext(ctx);
                    }
                }
            }


        } catch (IOException ex) {
            try {
                channel.close();
            } finally {
                key.cancel();
            }

            try {
                // Windows doesnt' seem to want to propagate a proper
                // ConnectionResetException..
                String message = ex.getMessage();
                if (message == null) {
                    throw ex;
                } else if (!(message.contains("reset") || message.contains("forcibly"))) {
                    throw ex;
                }
            } catch (ClosedChannelException exClosed) {
            }
            return;
        }

        int ioEvents = SelectionKey.OP_READ;
        if (client.hasOutput()) {
            ioEvents |= SelectionKey.OP_WRITE;
        }
        channel.register(selector, ioEvents, client);
    }

    public Bucket getBucket() {
        return bucket;
    }

    private boolean authOk(BinaryCommand cmd, MemcachedConnection client) {
        if (client.isAuthenticated()) {
            return true;
        }

        switch (cmd.getComCode()) {
            case SASL_AUTH:
            case SASL_LIST_MECHS:
            case SASL_STEP:
            case HELLO:
            case GET_ERRMAP:
                return true;

            default:
                return false;
        }
    }

    private CommandExecutor getExecutor(CommandCode code) {
        if (code == CommandCode.ILLEGAL) {
            return unknownHandler;
        }
        return executors[code.cc()];
    }

    @Override
    public void execute(BinaryCommand cmd, MemcachedConnection client)
            throws IOException {
        try {
            if (enhancedErrorsEnabled) {
                cmd.generateEventId();
            }

            if (shouldLogCommands) {
                commandLog.add(new CommandLogEntry(cmd.getOpcode()));
            }

            ErrorCode failcode = failmaker.getFailCode();
            if (failcode != ErrorCode.SUCCESS) {
                client.sendResponse(new BinaryResponse(cmd, failcode));
            } else if (authOk(cmd, client)) {
                long start = System.nanoTime();
                BinaryResponse response = getExecutor(cmd.getComCode()).execute(cmd, this, client);
                long end = System.nanoTime();
                if (response != null) {
                    if (client.supportsTracing()) {
                        long elapsedMicros = (end - start) / 1000;
                        long maxVal = 120125042;
                        elapsedMicros = Math.min(elapsedMicros, maxVal);
                        short encodedMicros = (short)Math.round(Math.pow(elapsedMicros * 2, 1.0 / 1.74));
                        response.getBuffer().get();
                        byte opcode = response.getBuffer().get();
                        short keyLength = response.getBuffer().getShort();
                        byte extraLength = response.getBuffer().get();
                        byte datatype = response.getBuffer().get();
                        short errorCode = response.getBuffer().getShort();
                        int bodyLength = response.getBuffer().getInt();
                        int opaque = response.getBuffer().getInt();
                        long cas = response.getBuffer().getLong();
                        byte framingExtrasLength = 3;
                        bodyLength += framingExtrasLength;
                        ByteBuffer message = ByteBuffer.allocate(24 + bodyLength);
                        message.put(BinaryResponse.ALT_MAGIC);
                        message.put(opcode);
                        message.put(framingExtrasLength);
                        message.put((byte)keyLength);
                        message.put(extraLength);
                        message.put(datatype);
                        message.putShort(errorCode);
                        message.putInt(bodyLength);
                        message.putInt(opaque);
                        message.putLong(cas);
                        byte tracing_framing_id = 0x02;
                        message.put(tracing_framing_id);
                        message.putShort(encodedMicros);
                        message.put(response.getBuffer());
                        message.rewind();
                        response.setBuffer(message);
                    }
                    client.sendResponse(response);
                }
            } else {
                client.sendResponse(new BinaryResponse(cmd, ErrorCode.AUTH_ERROR));
            }
        } catch (AccessControlException ex) {
            client.sendResponse(BinaryConfigResponse.createNotMyVbucket(cmd, this));
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

    public void flushNode() {
        storage.flush();
    }

    public void flushAll() {
        flushNode();
        for (MemcachedServer other : bucket.getServers()) {
            if (other == this) {
                continue;
            }
            other.flushNode();
        }
    }

    // Handy method
    public VBucketStore getCache(BinaryCommand cmd) {
        return storage.getCache(this, cmd.getVBucketId());
    }

    /**
     * Program entry point that runs the memcached server as a standalone
     * server just like any other memcached server...
     *
     * @param args Program arguments (not used)
     */
    public static void main(String[] args) {
        try {
            VBucketInfo vbi[] = new VBucketInfo[1024];
            for (int ii = 0; ii < vbi.length; ++ii) {
                vbi[ii] = new VBucketInfo();
            }
            MemcachedServer server = new MemcachedServer(null, null, 11211, vbi, false);
            for (VBucketInfo aVbi : vbi) {
                aVbi.setOwner(server);
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

    public boolean isCccpEnabled() {
        return cccpEnabled && bucket.getType() != BucketType.MEMCACHED;
    }

    public void setCccpEnabled(boolean enabled) {
        cccpEnabled = enabled;
    }

    /**
     * @return the type
     */
    public BucketType getType() {
        return bucket.getType();
    }

    public MemcachedConnection findConnection(SocketAddress address) throws IOException {
        for (SelectionKey key : selector.keys()) {
            Object o = key.attachment();
            if (o == null || !(o instanceof MemcachedConnection)) {
                continue;
            }
            SocketChannel ch = (SocketChannel) key.channel();
            if (ch.socket().getRemoteSocketAddress().equals(address)) {
                return (MemcachedConnection) o;
            }
        }
        return null;
    }

    public void startLog() {
        shouldLogCommands = true;
    }

    public void stopLog() {
        shouldLogCommands = false;
        commandLog.clear();
    }

    public List<CommandLogEntry> getLogs() {
        return commandLog;
    }
}
