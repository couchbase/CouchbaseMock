/*
 *  Copyright 2011 Couchbase, Inc..
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package org.couchbase.mock.memcached.protocol;

import java.net.ProtocolException;
import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 */
public class BinaryCommand {

    protected final ComCode cc;
    protected final short keylen;
    protected final byte extlen;
    protected final short vbucket;
    protected final int bodylen;
    protected final int opaque;
    protected final long cas;
    protected final ByteBuffer bodyBuffer;

    protected BinaryCommand(ByteBuffer header) throws ProtocolException {
        header.rewind();
        header.get(); // magic already validated
        cc = ComCode.valueOf(header.get());
        keylen = header.getShort();
        extlen = header.get();
        if (header.get() != 0) {
            throw new ProtocolException("Illegal datatype"); // illegal datatype
        }
        vbucket = header.getShort();
        bodylen = header.getInt();
        opaque = header.getInt();
        cas = header.getLong();
        if (bodylen > 0) {
            bodyBuffer = ByteBuffer.allocate(bodylen);
        } else {
            bodyBuffer = null;
        }
    }

    public ByteBuffer getInputBuffer() {
        return bodyBuffer;
    }

    public ComCode getComCode() {
        return cc;
    }

    /**
     * @return
     */
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
        return new String(bodyBuffer.array(), extlen, keylen);
    }

    public byte[] getValue() {
        byte ret[] = new byte[bodylen - extlen - keylen];
        System.arraycopy(bodyBuffer.array(), extlen + keylen, ret, 0, ret.length);
        return ret;
    }

    public boolean complete() {
        return bodylen == 0 || !bodyBuffer.hasRemaining();
    }
}
