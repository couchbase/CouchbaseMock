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
 * @author Sergey Avseyev <sergey.avseyev@gmail.com>
 */
public class BinaryStatResponse extends BinaryResponse {

    public BinaryStatResponse(BinaryCommand command, String key, String value) {
        super(create(command, key.getBytes(), value.getBytes()));
    }

    private static ByteBuffer create(BinaryCommand command, byte[] key, byte[] value) {
        final ByteBuffer message = BinaryResponse.create(command, ErrorCode.SUCCESS, 0, key.length, value.length, 0);
        message.put(key);
        message.put(value);
        message.rewind();
        return message;
    }
}
