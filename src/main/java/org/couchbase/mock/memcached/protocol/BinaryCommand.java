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

    protected final CommandCode cc;
    protected final short keyLength;
    protected final byte extraLength;
    protected final short vbucket;
    protected final int bodyLength;
    protected final int opaque;
    protected final long cas;
    protected final ByteBuffer bodyBuffer;

    protected BinaryCommand(ByteBuffer header) throws ProtocolException {
        header.rewind();
        header.get(); // magic already validated
        cc = CommandCode.valueOf(header.get());
        keyLength = header.getShort();
        extraLength = header.get();
        if (header.get() != 0) {
            throw new ProtocolException("Illegal datatype"); // illegal datatype
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

    public byte[] getValue() {
        byte ret[] = new byte[bodyLength - extraLength - keyLength];
        System.arraycopy(bodyBuffer.array(), extraLength + keyLength, ret, 0, ret.length);
        return ret;
    }

    public boolean complete() {
        return bodyLength == 0 || !bodyBuffer.hasRemaining();
    }
}
