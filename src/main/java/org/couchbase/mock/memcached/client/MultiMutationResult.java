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
 * Created by mnunberg on 10/12/15.
 */
public class MultiMutationResult {
    final private ErrorCode status;
    final private int index;
    final private String value;

    MultiMutationResult(ErrorCode status, int index, String value) {
        this.status = status;
        this.index = index;
        this.value = value;
    }

    public ErrorCode getStatus() {
        return status;
    }

    public int getIndex() {
        return index;
    }

    public String getValue() {
        return value;
    }

    public static List<MultiMutationResult> parse(ByteBuffer bb) {
        bb.rewind();
        List<MultiMutationResult> res = new ArrayList<MultiMutationResult>();
        while (bb.hasRemaining()) {
            int index = bb.get();
            ErrorCode ec = ErrorCode.valueOf(bb.getShort());
            String value = null;

            if (ec.value() == 0) {
                int valueLen = bb.getInt();
                byte[] valueBuf = new byte[valueLen];
                bb.get(valueBuf);
                value = new String(valueBuf);
            }
            res.add(new MultiMutationResult(ec, index, value));
        }
        return res;
    }
}
