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

import org.couchbase.mock.memcached.Item;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BinaryGetResponse extends BinaryResponse {
    public BinaryGetResponse(BinaryCommand command, ErrorCode error) {
        super(command, error);
    }

    public BinaryGetResponse(BinaryCommand command, Item item) {
        super(create(command, item));
    }

    private static ByteBuffer create(BinaryCommand command, Item item) {
        int keySize;
        switch (command.getComCode()) {
            case GETK:
            case GETKQ:
            case GET_REPLICA:
                keySize = command.getKey().length();
                break;
            default:
                keySize = 0;
        }
        final ByteBuffer message = BinaryResponse.create(command, ErrorCode.SUCCESS,
                4 /* flags */,
                keySize,
                item.getValue().length, item.getCas());
        message.putInt(item.getFlags());
        if (keySize > 0) {
            message.put(command.getKey().getBytes());
        }
        message.put(item.getValue());
        message.rewind();
        return message;
    }
}
