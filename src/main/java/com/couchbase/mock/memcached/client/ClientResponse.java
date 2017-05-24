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

package com.couchbase.mock.memcached.client;

import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.ErrorCode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by mnunberg on 1/15/14.
 */
public class ClientResponse {
    private CommandCode code;
    private ErrorCode status;
    private byte[] extras;
    private byte[] key;
    private byte[] value;
    private byte[] body;
    private int opaque;
    private byte opcode;

    long cas;

    public long getCas() {
        return cas;
    }

    public long getOpaque() {
        return opaque;
    }

    public String getKey() {
        return new String(key);
    }

    public String getValue() {
        return new String(value);
    }

    public ByteBuffer getRawValue() {
        return ByteBuffer.wrap(value);
    }

    public byte[] getExtras() {
        return extras;
    }

    public ErrorCode getStatus() {
        return status;
    }

    public byte getOpcode() {
        return opcode;
    }

    public CommandCode getComCode() {
        return code;
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
        ret.opcode = buf.get();
        ret.code = CommandCode.valueOf(ret.opcode);

        short keylen = buf.getShort();
        byte extlen = buf.get();
        buf.get(); // ignore datatype

        ret.status = ErrorCode.valueOf(buf.getShort());
        int totalLen = buf.getInt();
        ret.opaque = buf.getInt();
        ret.cas = buf.getLong();
        ret.body = new byte[totalLen];

        remaining = ret.body.length;
        while (remaining > 0) {
            int nr = input.read(ret.body, ret.body.length-remaining, remaining);
            if (nr < 0) {
                throw new IOException("Incomplete read");
            }
            remaining -= nr;
        }

        ret.extras = Arrays.copyOfRange(ret.body, 0, extlen);
        ret.key = Arrays.copyOfRange(ret.body, extlen, extlen + keylen);
        ret.value = Arrays.copyOfRange(ret.body, extlen + keylen, ret.body.length);
        return ret;
    }
}
