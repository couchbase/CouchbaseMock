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

import org.couchbase.mock.subdoc.Operation;

import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class BinarySubdocMultiCommand extends BinaryCommand {
    private int expiryTime;
    private boolean hasExpiry;
    protected final List<MultiSpec> specs = new ArrayList<MultiSpec>();

    public BinarySubdocMultiCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    @Override
    public void process() throws ProtocolException {
        bodyBuffer.rewind();
        if (extraLength == 0) {
            expiryTime = 0;
            hasExpiry = false;
        } else if (extraLength == 4) {
            expiryTime = bodyBuffer.getInt(0);
            hasExpiry = true;
        } else {
            throw new ProtocolException("Extras must be 0 or 4!");
        }

        try {
            bodyBuffer.position(extraLength + keyLength);
            extractSpecs();
        } finally {
            bodyBuffer.rewind();
        }

        if (specs.isEmpty()) {
            throw new ProtocolException("Found no specs!");
        }
    }

    protected abstract void extractSpecs() throws ProtocolException;

    public List<MultiSpec> getLookupSpecs() {
        return specs;
    }

    public int getNewExpiry(int oldExpiry) {
        if (hasExpiry) {
            return expiryTime;
        } else {
            return oldExpiry;
        }
    }

    public static class MultiSpec {
        final private Operation op;
        final private String path;
        final private String value;
        final private byte flags;

        protected MultiSpec(Operation op, String path, String value, byte flags) {
            this.op = op;
            this.path = path;
            this.flags = flags;
            this.value = value;
        }

        protected MultiSpec(Operation op, String path) {
            this(op, path, null, (byte)0);
        }

        public Operation getOp() {
            return op;
        }
        public String getPath() {
            return path;
        }
        public String getValue() {
            return value;
        }
        public byte getFlags() {
            return flags;
        }
    }
}
