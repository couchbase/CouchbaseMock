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
package com.couchbase.mock.memcached.protocol;

import java.net.ProtocolException;
import java.nio.ByteBuffer;

import com.couchbase.mock.memcached.KeySpec;

/**
 * @author Trond Norbye
 */
public class BinaryCommand {

    private final CommandCode cc;
    protected final short keyLength;
    protected final byte extraLength;
    protected final byte opcode;
    private final short vbucket;
    protected final int bodyLength;
    private final int opaque;
    final long cas;
    final ByteBuffer bodyBuffer;

    BinaryCommand(ByteBuffer header) throws ProtocolException {
        header.rewind();
        header.get(); // magic already validated
        opcode = header.get();
        cc = CommandCode.valueOf(opcode);
        keyLength = header.getShort();
        extraLength = header.get();
        if (header.get() != 0) {
            throw new ProtocolException("Illegal data type");
        }
        vbucket = header.getShort();
        bodyLength = header.getInt();
        opaque = header.getInt();
        cas = header.getLong();
        if (bodyLength > 0) {
            bodyBuffer = ByteBuffer.allocate(bodyLength);
        } else {
            bodyBuffer = null;
        }
    }

    public ByteBuffer getInputBuffer() {
        return bodyBuffer;
    }

    public CommandCode getComCode() {
        return cc;
    }

    public byte getOpcode() { return opcode; }

    public int getOpaque() {
        return opaque;
    }

    public short getVBucketId() {
        return vbucket;
    }

    public long getCas() {
        return cas;
    }

    public String getKey() {
        if (keyLength == 0) {
            return null;
        } else {
            return new String(bodyBuffer.array(), extraLength, keyLength);
        }
    }

    public KeySpec getKeySpec() {
        return new KeySpec(getKey(), vbucket);
    }

    public byte[] getValue() {
        byte ret[] = new byte[bodyLength - extraLength - keyLength];
        System.arraycopy(bodyBuffer.array(), extraLength + keyLength, ret, 0, ret.length);
        return ret;
    }

    public boolean complete() {
        return bodyLength == 0 || !bodyBuffer.hasRemaining();
    }


    /**
     * Any postprocessing on the body should be done here.
     * Used mainly for observe
     * @throws ProtocolException if a protocol error occurs
     */
    public void process() throws ProtocolException {
    }
}
