package org.couchbase.mock.memcached.client;

import org.couchbase.mock.memcached.protocol.BinaryHelloCommand;
import org.couchbase.mock.memcached.protocol.BinarySubdocCommand;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by mnunberg on 1/15/14.
 */
public class CommandBuilder {
    static int opaqueCounter = 0;
    private byte[] key = {};
    private byte[] value = {};
    private byte[] extras = {};
    private long cas = 0;
    private int opaque = opaqueCounter++;
    private short vbucket = 0;
    private final CommandCode command;

    public static class MultiLookupSpec {
        final String path;
        final CommandCode op;
        public MultiLookupSpec(CommandCode op, String path) {
            this.path = path;
            this.op = op;
        }

        public static MultiLookupSpec exists(String path) {
            return new MultiLookupSpec(CommandCode.SUBDOC_EXISTS, path);
        }

        public static MultiLookupSpec get(String path) {
            return new MultiLookupSpec(CommandCode.SUBDOC_GET, path);
        }
    }

    public static class MultiMutationSpec {
        final CommandCode op;
        final byte flags;
        final String path;
        final String value;
        public MultiMutationSpec(CommandCode op, String path, String value, int flags) {
            this.op = op;
            this.path = path;
            this.value = value;
            this.flags = (byte)flags;
        }
        public MultiMutationSpec(CommandCode op, String path, String value, boolean create) {
            this(op, path, value, create ? BinarySubdocCommand.FLAG_MKDIR_P : 0x0);
        }
        public MultiMutationSpec(CommandCode op, String path, String value) {
            this(op, path, value, false);
        }
        public MultiMutationSpec(CommandCode op, String path) {
            this(op, path, null, (byte)0x0);
        }
    }

    public CommandBuilder(CommandCode command) {
        this.command = command;
    }

    public CommandBuilder key(String key, short vbucket) {
        this.key = key.getBytes();
        this.vbucket = vbucket;
        return this;
    }

    public CommandBuilder vBucket(short vbucket) {
        this.vbucket = vbucket;
        return this;
    }

    public CommandBuilder value(String value) {
        this.value = value.getBytes();
        return this;
    }

    public CommandBuilder value(byte[] value) {
        this.value = value;
        return this;
    }

    public CommandBuilder cas(long cas) {
        this.cas = cas;
        return this;
    }

    public CommandBuilder extras(byte[] extras) {
        this.extras = extras;
        return this;
    }

    public CommandBuilder value(byte[] value, int flags) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(flags);
        bb.rewind();
        extras(bb.array());
        value(value);
        return this;
    }

    public CommandBuilder subdoc(byte[] sdPath, byte[] sdValue, int sdFlags, int expiry) {
        int numExtras = expiry == 0 ? 3 : 7;
        ByteBuffer extrasBuf = ByteBuffer.allocate(numExtras);
        extrasBuf.putShort((short)sdPath.length);
        extrasBuf.put((byte)sdFlags);

        if (expiry != 0) {
            extrasBuf.putInt(expiry);
        }

        if (sdValue == null) {
            sdValue = new byte[0];
        }

        ByteBuffer valuePack = ByteBuffer.allocate(sdPath.length + sdValue.length);
        valuePack.put(sdPath);
        valuePack.put(sdValue);

        extras(extrasBuf.array());
        value(valuePack.array());
        return this;
    }

    public CommandBuilder subdoc(byte[] sdPath) {
        return subdoc(sdPath, null, 0, 0);
    }

    public CommandBuilder subdoc(byte[] sdPath, byte[] sdValue) {
        return subdoc(sdPath, sdValue, 0, 0);
    }
    public CommandBuilder subdoc(String sdPath, String sdValue) {
        return subdoc(sdPath.getBytes(), sdValue.getBytes());
    }

    public CommandBuilder subdoc(byte[] sdPath, byte[] sdValue, int sdFlags) {
        return subdoc(sdPath, sdValue, sdFlags, 0);
    }

    public CommandBuilder subdoc(String sdPath, String sdValue, int sdFlags) {
        return subdoc(sdPath.getBytes(), sdValue.getBytes(), sdFlags);
    }

    private static byte[] subdocMultiLookupPayload(MultiLookupSpec[] specs) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        for (MultiLookupSpec spec : specs) {
            ByteBuffer bb = ByteBuffer.allocate(1 + 1 + 2 + spec.path.length());
            bb.put((byte)spec.op.cc());
            bb.put((byte)0x00);
            bb.putShort((short)spec.path.getBytes().length);
            bb.put(spec.path.getBytes());
            try {
                bao.write(bb.array());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return bao.toByteArray();
    }

    public CommandBuilder subdocMultiLookup(int expiry, MultiLookupSpec... specs) {
        if (expiry != -1) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(expiry);
            extras(bb.array());
        }

        value(subdocMultiLookupPayload(specs));
        return this;
    }

    public CommandBuilder subdocMultiLookup(MultiLookupSpec... specs) {
        return subdocMultiLookup(-1, specs);
    }

    private static byte[] subdocMultiMutationPayload(MultiMutationSpec[] specs) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        for (MultiMutationSpec spec : specs) {
            String value = spec.value;
            if (value == null) {
                value = "";
            }
            ByteBuffer bb = ByteBuffer.allocate(1 + 1 + 2 + 4 +
                    spec.path.length() + value.length());

            bb.put((byte)spec.op.cc());
            bb.put(spec.flags);
            bb.putShort((short)spec.path.getBytes().length);
            bb.putInt(value.length());
            bb.put(spec.path.getBytes());
            bb.put(value.getBytes());
            try {
                bao.write(bb.array());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return bao.toByteArray();
    }

    public CommandBuilder subdocMultiMutation(int expiry, MultiMutationSpec... specs) {
        if (expiry != -1) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(expiry);
            extras(bb.array());
        }
        value(subdocMultiMutationPayload(specs));
        return this;
    }

    public CommandBuilder subdocMultiMutation(MultiMutationSpec... specs) {
        return subdocMultiMutation(-1, specs);
    }

    public static byte[] buildHello(String name, BinaryHelloCommand.Feature... features) {
        CommandBuilder cBuilder = new CommandBuilder(CommandCode.HELLO);
        cBuilder.key(name, (short)0);
        ByteBuffer bb = ByteBuffer.allocate(features.length * 2);
        for (BinaryHelloCommand.Feature f : features) {
            bb.putShort((short)f.getValue());
        }
        bb.rewind();
        cBuilder.value(bb.array());
        return cBuilder.build();
    }

    public static byte[] buildStore(String key, short vbucket, String value) {
        CommandBuilder cBuilder = new CommandBuilder(CommandCode.SET);
        cBuilder.key(key, vbucket);
        cBuilder.value(value.getBytes(), 0);
        return cBuilder.build();
    }

    public static byte[] buildPlainAuth(String username, String password) {
        byte[] bUsername = username.getBytes();
        byte[] bPassword = password.getBytes();

        ByteBuffer buf = ByteBuffer.allocate(bUsername.length + bPassword.length + 2);
        buf.put((byte)0x00);
        buf.put(bUsername);
        buf.put((byte)0x00);
        buf.put(bPassword);

        return new CommandBuilder(CommandCode.SASL_AUTH)
                .key("PLAIN", (short)0)
                .value(buf.array())
                .build();
    }

    public static byte[] buildSubdocGet(String key, short vbucket, String path) {
        return new CommandBuilder(CommandCode.SUBDOC_GET)
                .key(key, vbucket)
                .subdoc(path.getBytes())
                .build();
    }

    public byte[] build() {
        int totalLen = 24 + key.length + value.length + extras.length;
        byte[] ret = new byte[totalLen];
        ByteBuffer buffer = ByteBuffer.wrap(ret);

        // Magic: PROTOCOL_BINARY_REQ
        buffer.put((byte) 0x80);

        // Opcode
        buffer.put((byte) command.cc());

        // Key Length
        buffer.putShort((short) key.length);

        buffer.put((byte)extras.length);

        // Datatype. Ignored
        buffer.put((byte) 0);


        // Vbucket
        buffer.putShort(vbucket);

        // Body length
        buffer.putInt(totalLen - 24);

        // Opaque
        buffer.putInt(opaque);


        // CAS
        buffer.putLong(cas);

        // Stuff the body..
        buffer.put(extras);
        buffer.put(key);
        buffer.put(value);
        return ret;
    }
}
