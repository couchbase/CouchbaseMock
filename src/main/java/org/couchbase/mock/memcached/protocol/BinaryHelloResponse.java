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

import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.memcached.client.MemcachedClient;

/**
 * Created by mnunberg on 2/4/15.
 */
public class BinaryHelloResponse extends BinaryResponse {
    public BinaryHelloResponse(BinaryHelloCommand cmd, int[] supported) {
        super(cmd, ErrorCode.SUCCESS, 0, 0, supported.length * 2, 0);
        for (int i = 0; i < supported.length; i++) {
            buffer.putShort(24 + (i * 2), (short)supported[i]);
        }
    }
}
