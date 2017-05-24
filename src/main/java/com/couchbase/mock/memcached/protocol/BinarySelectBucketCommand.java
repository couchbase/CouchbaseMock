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

import org.jetbrains.annotations.NotNull;

import java.net.ProtocolException;
import java.nio.ByteBuffer;

/**
 * Created by mnunberg on 3/3/17.
 */
public class BinarySelectBucketCommand extends BinaryCommand {
    public BinarySelectBucketCommand(ByteBuffer header) throws ProtocolException {
        super(header);
        if (getKey() == null || getKey().isEmpty()) {
            throw new ProtocolException("Key must not be empty");
        }
    }

    public @NotNull String getBucketName() {
        return getKey();
    }
}
