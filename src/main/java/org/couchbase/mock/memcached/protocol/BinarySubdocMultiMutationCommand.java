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

import java.net.ProtocolException;
import java.nio.ByteBuffer;

/**
 * Created by mnunberg on 10/9/15.
 */
public class BinarySubdocMultiMutationCommand extends BinarySubdocMultiCommand {
    public BinarySubdocMultiMutationCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    @Override
    protected void extractSpecs() throws ProtocolException {
        while (bodyBuffer.hasRemaining()) {
            byte bOp = bodyBuffer.get();
            byte flags = bodyBuffer.get();
            short pathLength = bodyBuffer.getShort();
            int valueLength = bodyBuffer.getInt();
            byte[] path = new byte[pathLength];
            byte[] value = new byte[valueLength];
            bodyBuffer.get(path);
            bodyBuffer.get(value);
            specs.add(new MultiSpec(bOp, new String(path), new String(value), flags));
        }
    }
}
