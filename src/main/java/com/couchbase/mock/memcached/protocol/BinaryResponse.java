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

import com.couchbase.mock.memcached.MutationInfoWriter;
import com.couchbase.mock.memcached.MutationStatus;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 */
public class BinaryResponse {
    public static final byte MAGIC = (byte) 0x81;
    public static final byte ALT_MAGIC = (byte) 0x18;
    ByteBuffer buffer;

    BinaryResponse(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    BinaryResponse(BinaryCommand command, ErrorCode errorCode, byte datatype, int extraLength, int keyLength, int dataLength, long cas) {
        buffer = createAndRewind(command, errorCode, datatype, extraLength, keyLength, dataLength, cas);
    }

    public BinaryResponse(BinaryCommand command, ErrorCode errorCode, String errorContext) {
        if (command.getEventId() != null || errorContext != null) {
            JsonObject error = new JsonObject();
            if (command.getEventId() != null) {
                error.addProperty("ref", command.getEventId());
            }
            if (errorContext != null) {
                error.addProperty("context", errorContext);
            }
            JsonObject body = new JsonObject();
            body.add("error", error);
            byte[] value = new Gson().toJson(body).getBytes();
            buffer = create(command, errorCode,  Datatype.RAW.value(), 0, 0, value.length, 0);
            buffer.position(5);
            buffer.put(Datatype.JSON.value());
            buffer.position(24);
            buffer.put(value);
            buffer.rewind();
        } else {
            buffer = createAndRewind(command, errorCode,  Datatype.RAW.value(), 0, 0, 0, 0);
        }
    }

    public BinaryResponse(BinaryCommand command, ErrorCode errorCode) {
        this(command, errorCode, null);
    }

    public BinaryResponse(BinaryCommand command, MutationStatus ms, MutationInfoWriter miw, long cas) {
        this(command, ms, miw, Datatype.RAW.value(), cas, null);
    }

    public static BinaryResponse createWithValue(BinaryCommand command, byte datatype, byte[] value, long cas) {
        return createWithValue(ErrorCode.SUCCESS, command, datatype, value, cas);
    }

    public static BinaryResponse createWithValue(ErrorCode ec, BinaryCommand command, byte datatype, byte[] value, long cas) {
        int vallen = value == null ? 0 : value.length;
        BinaryResponse resp = new BinaryResponse(command, ec, datatype,0,  0, vallen, cas);
        if (vallen > 0) {
            resp.buffer.position(24);
            resp.buffer.put(value);
        }
        resp.buffer.rewind();
        return resp;
    }

    public BinaryResponse(BinaryCommand command, MutationStatus ms, MutationInfoWriter miw, byte datatype, long cas, byte[] value) {
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

        buffer = createAndRewind(command, ms.getStatus(), datatype, extlen, 0, valLen, cas);
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

    private static ByteBuffer createAndRewind(BinaryCommand command, ErrorCode errorCode, byte datatype, int extraLength, int keyLength, int dataLength, long cas) {
        ByteBuffer message = create(command, errorCode, datatype, extraLength, keyLength, dataLength, cas);
        message.rewind();
        return message;
    }

    static ByteBuffer create(BinaryCommand command, ErrorCode errorCode, byte datatype, int extraLength, int keyLength, int dataLength, long cas) {
        ByteBuffer message = ByteBuffer.allocate(24 + extraLength + keyLength + dataLength);
           message.put(MAGIC);
           message.put(command.getOpcode());
           message.putShort((short)keyLength);
           message.put((byte)extraLength);
           message.put(datatype);
           message.putShort(errorCode.value());
           message.putInt(dataLength + keyLength + extraLength);
           message.putInt(command.getOpaque());
           message.putLong(cas);
        return message;
    }

    public ErrorCode getErrorCode() {
        return ErrorCode.valueOf(buffer.getShort(6));
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
