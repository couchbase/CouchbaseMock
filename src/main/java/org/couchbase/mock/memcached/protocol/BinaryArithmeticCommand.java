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

import java.net.ProtocolException;
import java.nio.ByteBuffer;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class BinaryArithmeticCommand extends BinaryCommand {

    public BinaryArithmeticCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    public long getDelta() {
        return bodyBuffer.getLong(0);
    }

    public long getInitial() {
        return bodyBuffer.getLong(8);
    }

    public int getExpiration() {
        return bodyBuffer.getInt(16);
    }

    public boolean create() {
        return getExpiration() != 0xffffffff;
    }
}
