package org.couchbase.mock.memcached.client;

import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by mnunberg on 1/15/14.
 */
public class ClientResponse {
    private CommandCode code;
    private ErrorCode status;
    private ByteBuffer extras;
    private ByteBuffer key;
    private ByteBuffer value;
    private byte[] body;
    private int opaque;

    long cas;

    public long getCas() {
        return cas;
    }

    public long getOpaque() {
        return opaque;
    }

    public String getKey() {
        return new String(key.array());
    }

    public String getValue() {
        return new String(value.array());
    }

    public ByteBuffer getRawValue() {
        return value.asReadOnlyBuffer();
    }

    public byte[] getExtras() {
        return extras.array();
    }

    public ErrorCode getStatus() {
        return status;
    }

    public boolean success() { return status == ErrorCode.SUCCESS; }


    public static ClientResponse read(InputStream input) throws IOException {
        byte[] header = new byte[24];
        int remaining = header.length;

        while (remaining > 0) {
            int nr = input.read(header, header.length-remaining, remaining);
            if (nr == -1) {
                break;
            }
            remaining -= nr;
        }

        if (remaining > 0) {
            throw new IOException("Incomplete read before stream closed");
        }

        ByteBuffer buf = ByteBuffer.wrap(header);
        byte magic = buf.get();
        if (magic != (byte)0x81) {
            throw new IOException("Illegal magic: " + magic);
        }

        ClientResponse ret = new ClientResponse();
        ret.code = CommandCode.valueOf(buf.get());
        if (ret.code == CommandCode.ILLEGAL) {
            throw new IOException("Illegal command");
        }


        short keylen = buf.getShort();
        byte extlen = buf.get();
        buf.get(); // ignore datatype

        ret.status = ErrorCode.valueOf(buf.getShort());
        int totalLen = buf.getInt();
        ret.opaque = buf.getInt();
        ret.cas = buf.getLong();
        ret.body = new byte[totalLen];
        ret.extras = ByteBuffer.wrap(ret.body, 0, extlen);
        ret.key = ByteBuffer.wrap(ret.body, extlen, keylen);
        ret.value = ByteBuffer.wrap(ret.body, extlen+keylen, totalLen - (extlen + keylen));

        remaining = ret.body.length;
        while (remaining > 0) {
            int nr = input.read(ret.body, ret.body.length-remaining, remaining);
            if (nr < 0) {
                throw new IOException("Incomplete read");
            }
            remaining -= nr;
        }

        return ret;
    }
}
