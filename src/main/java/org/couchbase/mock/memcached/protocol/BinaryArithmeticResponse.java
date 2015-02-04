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

import org.couchbase.mock.memcached.MutationInfoWriter;
import org.couchbase.mock.memcached.MutationStatus;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BinaryArithmeticResponse extends BinaryResponse {

    public BinaryArithmeticResponse(BinaryArithmeticCommand command, long val, long cas, MutationStatus ms, MutationInfoWriter miw) {
        super(command, ErrorCode.SUCCESS, miw.extrasLength(), 0, 8, cas);

        if (miw.extrasLength() != 0) {
            miw.write(buffer, ms.getCoords());
        }

        buffer.putLong(24 + miw.extrasLength(), val);
        buffer.rewind();
    }
}
