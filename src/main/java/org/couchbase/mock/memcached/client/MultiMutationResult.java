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

package org.couchbase.mock.memcached.client;

import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.nio.ByteBuffer;

/**
 * Created by mnunberg on 10/12/15.
 */
public class MultiMutationResult {
    final private ErrorCode status;
    final private int errorIndex;

    MultiMutationResult(ErrorCode status, int index) {
        this.status = status;
        this.errorIndex = index;
    }

    public ErrorCode getStatus() {
        return status;
    }

    public int getErrorIndex() {
        return errorIndex;
    }

    public static MultiMutationResult parse(ByteBuffer bb) {
        bb.rewind();
        ErrorCode ec = ErrorCode.valueOf(bb.getShort());
        int index = bb.get();
        return new MultiMutationResult(ec, index);
    }
}
