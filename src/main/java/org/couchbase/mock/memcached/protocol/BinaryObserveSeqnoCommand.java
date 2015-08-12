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
 * Created by mnunberg on 2/4/15.
 */
public class BinaryObserveSeqnoCommand extends BinaryCommand {
    public BinaryObserveSeqnoCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    private long uuid = 0;

    @Override
    public void process() throws ProtocolException {
        super.process();
        bodyBuffer.position(0);
        uuid = bodyBuffer.getLong();
    }

    public long getUuid() {
        return uuid;
    }
}
