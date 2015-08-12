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
import java.util.Arrays;

/**
 * Created by mnunberg on 2/4/15.
 */
public class BinaryHelloCommand extends BinaryCommand {
    private boolean isProcessed = false;
    private boolean[] features = new boolean[Feature.MAX.getValue()];

    public BinaryHelloCommand(ByteBuffer header) throws ProtocolException {
        super(header);
    }

    @Override
    public void process() throws ProtocolException {
        if (isProcessed) {
            return;
        }

        ByteBuffer bb = ByteBuffer.wrap(getValue());
        while (bb.hasRemaining()) {
            int feature = bb.getShort();
            if (feature < 0 || feature > Feature.MAX.value - 1) {
                continue;
            }
            features[feature] = true;
        }
        isProcessed = true;
    }

    public boolean[] getFeatures() {
        return Arrays.copyOf(features, features.length);
    }

    public enum Feature {
        DATATYPE (1), TLS (2), TCP_NODELAY (3), MUTATION_SEQNO (4), MAX (5);

        private final int value;
        private Feature(int value) {
            this.value = value;
        }
        public int getValue() {
            return value;
        }
    }

}
