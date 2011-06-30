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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 */
public class BinaryCommand {

    private final ComCode cc;
    private final short keylen;
    private final byte extlen;
    private final short vbucket;
    private final int bodylen;
    private final int opaque;
    private final long cas;
    private final ByteBuffer bodyBuffer;

    public BinaryCommand(ByteBuffer header) throws IOException {
        header.rewind();
        if (header.get() != (byte)0x80) {
            // create a better one... this is an illegal command
            throw new IOException();
        }
        cc = ComCode.valueOf(header.get());
        keylen = header.getShort();
        extlen = header.get();
        if (header.get() != 0) {
            throw new IOException(); // illegal datatype
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

    public Item getItem() {

        switch (cc) {
            case ADD:
            case REPLACE:
            case SET:
            case ADDQ:
            case REPLACEQ:
            case SETQ:
                break;
            default:
                throw new RuntimeException("Illegall command to call getItem for");
        }

        int flags = bodyBuffer.getInt(0);
        int exptime = bodyBuffer.getInt(4);

        return new Item(getKey(), flags, exptime, getValue(), cas);
    }

    boolean complete() {
        return bodylen == 0 || !bodyBuffer.hasRemaining();
    }
}
