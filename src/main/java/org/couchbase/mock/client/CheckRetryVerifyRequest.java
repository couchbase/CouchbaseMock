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

package org.couchbase.mock.client;

import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Created by mnunberg on 4/24/17.
 */
public class CheckRetryVerifyRequest extends MockRequest {
    public CheckRetryVerifyRequest(int index, String bucket, CommandCode opcode, ErrorCode errcode) {
        super();
        setName("check_retry_verify");
        payload.put("idx", index);
        payload.put("opcode", opcode.cc());
        payload.put("errcode", errcode.value());
        if (bucket != null) {
            payload.put("bucket", bucket);
        }
    }
}
