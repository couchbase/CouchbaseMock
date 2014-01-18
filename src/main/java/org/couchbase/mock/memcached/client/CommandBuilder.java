package org.couchbase.mock.memcached.client;

import org.couchbase.mock.memcached.protocol.CommandCode;

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

    public CommandBuilder(CommandCode command) {
        this.command = command;
    }

    public CommandBuilder key(String key, short vbucket) {
        this.key = key.getBytes();
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
