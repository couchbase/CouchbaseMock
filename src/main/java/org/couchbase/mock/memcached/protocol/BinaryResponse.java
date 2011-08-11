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

import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 *
 */
public class BinaryResponse {
    private static final byte MAGIC = (byte)0x81;
    private static final byte DATATYPE = 0;
    protected final ByteBuffer buffer;

    protected BinaryResponse(BinaryCommand command, ErrorCode errorCode, int extlen, int keylen, int datalen, long cas) {
        buffer = create(command, errorCode, extlen, keylen, datalen, cas);
    }

    public BinaryResponse(BinaryCommand command, ErrorCode errorCode) {
        buffer = create(command, errorCode, 0, 0, 0, 0);
    }

    private ByteBuffer create(BinaryCommand command, ErrorCode errorCode, int extlen, int keylen, int datalen, long cas) {
       ByteBuffer message = ByteBuffer.allocate(24 + extlen + keylen + datalen);
       message.put(MAGIC);
       message.put(command.getComCode().cc());
       message.putShort((short)keylen);
       message.put((byte)extlen);
       message.put(DATATYPE);
       message.putShort(errorCode.value());
       message.putInt(datalen + keylen + extlen);
       message.putInt(command.getOpaque());
       message.putLong(cas);
       message.rewind();
       return message;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
