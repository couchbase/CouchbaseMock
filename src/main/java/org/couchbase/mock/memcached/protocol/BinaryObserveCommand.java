/*
 * Copyright 2013 Couchbase.
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

import java.net.ProtocolException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.couchbase.mock.memcached.KeySpec;

/**
 * This implements the OBSERVE request.
 *
 * The OBSERVE request contains empty
 * fields for its key, value, and vBucket, with its payload being a packed
 * sequence of (vb, key length, key) triples.
 *
 * See:
 *  https://github.com/membase/ep-engine/blob/2.0.0/src/ep_engine.cc#L3431
 *  http://www.couchbase.com/wiki/display/couchbase/Observe
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class BinaryObserveCommand extends BinaryCommand {
    private final List<KeySpec> keySpecs = new ArrayList<KeySpec>();

    protected BinaryObserveCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    public List<KeySpec> getKeySpecs() {
        return new ArrayList<KeySpec>(keySpecs);
    }


    @Override
    public void process() throws ProtocolException {
        if (keySpecs.size() > 0) {
            return;
        }
        bodyBuffer.rewind();
        while (bodyBuffer.hasRemaining()) {
            try {
                short vb = bodyBuffer.getShort();
                short keyLength = bodyBuffer.getShort();
                byte[] keyBuffer = new byte[keyLength];
                bodyBuffer.get(keyBuffer);
                String key = new String(keyBuffer);
                KeySpec ks = new KeySpec(key, vb);
                keySpecs.add(ks);

            } catch (BufferUnderflowException e) {
                throw new ProtocolException();
            }
        }
    }
}