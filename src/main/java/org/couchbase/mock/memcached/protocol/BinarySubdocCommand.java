/*
 * Copyright 2015 Couchbase, Inc.
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

package org.couchbase.mock.memcached.protocol;

import org.couchbase.mock.memcached.SubdocItem;
import org.couchbase.mock.subdoc.Operation;

import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BinarySubdocCommand extends BinaryCommand {

    private final Operation subdocOp;
    public final static byte FLAG_MKDIR_P = 0x01;
    public final static byte FLAG_MKDOC = 0x02;

    private final static Map<CommandCode, Operation> opMap = new HashMap<CommandCode, Operation>();
    static {
        opMap.put(CommandCode.SUBDOC_GET, Operation.GET);
        opMap.put(CommandCode.SUBDOC_EXISTS, Operation.EXISTS);
        opMap.put(CommandCode.SUBDOC_DICT_ADD, Operation.DICT_ADD);
        opMap.put(CommandCode.SUBDOC_DICT_UPSERT, Operation.DICT_UPSERT);
        opMap.put(CommandCode.SUBDOC_DELETE, Operation.REMOVE);
        opMap.put(CommandCode.SUBDOC_REPLACE, Operation.REPLACE);
        opMap.put(CommandCode.SUBDOC_ARRAY_PUSH_LAST, Operation.ARRAY_APPEND);
        opMap.put(CommandCode.SUBDOC_ARRAY_PUSH_FIRST, Operation.ARRAY_PREPEND);
        opMap.put(CommandCode.SUBDOC_ARRAY_INSERT, Operation.ARRAY_INSERT);
        opMap.put(CommandCode.SUBDOC_ARRAY_ADD_UNIQUE, Operation.ADD_UNIQUE);
        opMap.put(CommandCode.SUBDOC_COUNTER, Operation.COUNTER);
        opMap.put(CommandCode.SUBDOC_GET_COUNT, Operation.GET_COUNT);
    }

    BinarySubdocCommand(ByteBuffer header) throws ProtocolException {
        super(header);
        if (extraLength != 3 && extraLength != 7) {
            throw new ProtocolException("Extras must be 3 or 7");
        }

        subdocOp = toSubdocOpcode(getComCode());
        if (subdocOp == null) {
            throw new ProtocolException("Unhandled opcode: " + getComCode());
        }
    }

    public static Operation toSubdocOpcode(CommandCode op) {
        return opMap.get(op);
    }

    public byte getSubdocFlags() {
        return bodyBuffer.get(2);
    }

    public Operation getSubdocOp() {
        return subdocOp;
    }

    public SubdocItem getItem() {
        int expiryTime = 0;
        if (extraLength == 7) {
            expiryTime = bodyBuffer.getInt(3);
        }

        int pathLength = bodyBuffer.getShort(0);
        byte[] path = new byte[pathLength];

        // Seek and read into buffer for path
        bodyBuffer.position(extraLength + keyLength);
        bodyBuffer.get(path);

        int valueLength = bodyLength - (keyLength + pathLength + extraLength);
        byte[] value = new byte[valueLength];

        bodyBuffer.position(extraLength + keyLength + pathLength);
        bodyBuffer.get(value);

        return new SubdocItem(getKeySpec(), 0, expiryTime, path, value, cas);
    }
}
