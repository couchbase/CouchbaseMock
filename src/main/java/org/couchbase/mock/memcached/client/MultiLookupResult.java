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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mnunberg on 10/9/15.
 */
public class MultiLookupResult {
    final private ErrorCode status;
    final private String value;

    MultiLookupResult(ErrorCode status, String value) {
        this.status = status;
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public ErrorCode getStatus() {
        return status;
    }

    public boolean success() {
        return status == ErrorCode.SUCCESS;
    }

    public static List<MultiLookupResult> parse(ByteBuffer buf) {
        List<MultiLookupResult> results = new ArrayList<MultiLookupResult>();
        buf.rewind();

        while (buf.hasRemaining()) {
            short status = buf.getShort();
            ErrorCode ec = ErrorCode.valueOf(status);
            int valueLength = buf.getInt();
            byte[] vBytes = new byte[valueLength];
            buf.get(vBytes);
            String value = new String(vBytes);
            results.add(new MultiLookupResult(ec, value));
        }
        return results;
    }
}
