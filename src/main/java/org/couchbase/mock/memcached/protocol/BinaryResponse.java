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

import org.couchbase.mock.memcached.MutationInfoWriter;
import org.couchbase.mock.memcached.MutationStatus;

import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 */
public class BinaryResponse {
    private static final byte MAGIC = (byte) 0x81;
    private static final byte DATA_TYPE = 0;
    final ByteBuffer buffer;

    BinaryResponse(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    BinaryResponse(BinaryCommand command, ErrorCode errorCode, int extraLength, int keyLength, int dataLength, long cas) {
        buffer = createAndRewind(command, errorCode, extraLength, keyLength, dataLength, cas);
    }

    public BinaryResponse(BinaryCommand command, ErrorCode errorCode) {
        buffer = createAndRewind(command, errorCode, 0, 0, 0, 0);
    }

    public BinaryResponse(BinaryCommand command, MutationStatus ms, MutationInfoWriter miw, long cas) {
        this(command, ms, miw, cas, null);
    }

    public static BinaryResponse createWithValue(BinaryCommand command, byte[] value, long cas) {
        return createWithValue(ErrorCode.SUCCESS, command, value, cas);
    }

    public static BinaryResponse createWithValue(ErrorCode ec, BinaryCommand command, byte[] value, long cas) {
        int vallen = value == null ? 0 : value.length;
        BinaryResponse resp = new BinaryResponse(command, ec, 0, 0, vallen, cas);
        if (vallen > 0) {
            resp.buffer.position(24);
            resp.buffer.put(value);
        }
        resp.buffer.rewind();
        return resp;
    }

    public BinaryResponse(BinaryCommand command, MutationStatus ms, MutationInfoWriter miw, long cas, byte[] value) {
        int extlen = 0;
        int valLen = 0;
        if (value != null) {
            valLen = value.length;
        }
        boolean shouldWrite = false;
        if (ms.getStatus().value() == ErrorCode.SUCCESS.value()) {
            extlen = miw.extrasLength();
            shouldWrite = true;
        }

        buffer = createAndRewind(command, ms.getStatus(), extlen, 0, valLen, cas);
        buffer.position(24);
        if (shouldWrite && extlen != 0) {
            miw.write(buffer, ms.getCoords());
        }
        if (value != null) {
            buffer.position(extlen + 24);
            buffer.put(value);
        }
        buffer.rewind();
    }

    private static ByteBuffer createAndRewind(BinaryCommand command, ErrorCode errorCode, int extraLength, int keyLength, int dataLength, long cas) {
        ByteBuffer message = create(command, errorCode, extraLength, keyLength, dataLength, cas);
        message.rewind();
        return message;
    }

    static ByteBuffer create(BinaryCommand command, ErrorCode errorCode, int extraLength, int keyLength, int dataLength, long cas) {
        ByteBuffer message = ByteBuffer.allocate(24 + extraLength + keyLength + dataLength);
           message.put(MAGIC);
           message.put(command.getOpcode());
           message.putShort((short)keyLength);
           message.put((byte)extraLength);
           message.put(DATA_TYPE);
           message.putShort(errorCode.value());
           message.putInt(dataLength + keyLength + extraLength);
           message.putInt(command.getOpaque());
           message.putLong(cas);
        return message;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
