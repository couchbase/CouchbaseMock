/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.memcached.protocol;

import java.nio.ByteBuffer;
import java.util.List;
import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.KeySpec;
import org.couchbase.mock.memcached.ObsKeyState;

/**
 * This contains the response for the OBSERVE command.
 * The response's key and cas field are set to 0, while the body
 * field contains 5-tuples of (vb, key length, key, status, cas)
 *
 * The status for this command is always successful.
 *
 * See the BinaryObserveCommand for more information.
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class BinaryObserveResponse extends BinaryResponse {

    private static int calculateLength(List<ObsKeyState> states) {
        int len = 0;
        for (ObsKeyState ks : states) {
            len += 13; // CAS + vBucket + status + key length;
            len += ks.key.length();
        }
        return len;
    }

    private static ByteBuffer create(BinaryCommand command, List<ObsKeyState> states) {
        int len = calculateLength(states);
        final ByteBuffer message = BinaryResponse.create(
                command, ErrorCode.SUCCESS, 0, 0, len, 0);

        for (ObsKeyState ks : states) {
            message.putShort(ks.vbId);
            message.putShort((short)ks.key.length());
            message.put(ks.key.getBytes());
            message.put((byte)ks.status.getValue());
            message.putLong(ks.cas);
        }
        message.rewind();
        return message;
    }

    public BinaryObserveResponse(BinaryCommand command, List<ObsKeyState> states) {
        super(create(command, states));
    }
}
