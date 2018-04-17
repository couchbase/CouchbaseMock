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

import java.nio.ByteBuffer;

/**
 * @author Sergey Avseyev
 */
public class BinarySaslResponse extends BinaryResponse {

    public BinarySaslResponse(BinaryCommand command) {
        super(command, ErrorCode.AUTH_ERROR);
    }

    public BinarySaslResponse(BinaryCommand command, String data) {
        this(command, data, ErrorCode.SUCCESS);
    }

    public BinarySaslResponse(BinaryCommand command, String data, ErrorCode errorCode) {
        super(create(command, data, errorCode));
    }

    private static ByteBuffer create(BinaryCommand command, String data, ErrorCode errorCode) {
        final ByteBuffer message = BinaryResponse.create(command,
                errorCode, Datatype.RAW.value(), 0, 0, data.getBytes().length, 0);
        message.put(data.getBytes());
        message.rewind();
        return message;
    }
}
