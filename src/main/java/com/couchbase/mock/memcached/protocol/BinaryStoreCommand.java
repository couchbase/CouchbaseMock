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

import com.couchbase.mock.memcached.CompressionMode;
import com.couchbase.mock.memcached.Item;
import org.iq80.snappy.Snappy;

import java.net.ProtocolException;
import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 */
public class BinaryStoreCommand extends BinaryCommand {
    public BinaryStoreCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    public Item getItem(CompressionMode snappyMode) throws ProtocolException {
        int flags = 0, expiryTime = 0;

        if (extraLength == 8) {
            flags = bodyBuffer.getInt(0);
            expiryTime = bodyBuffer.getInt(4);
        }
        byte[] value = getValue();
        if ((datatype & Datatype.SNAPPY.value()) > 0) {
            if (snappyMode == CompressionMode.OFF) {
                throw new ProtocolException("Cannot handle compressed data");
            } else {
                value = Snappy.uncompress(value, 0, value.length);
            }
        }

        return new Item(getKeySpec(), flags, expiryTime, value, null, cas, datatype);
    }
}
