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
 * Helper class to create the correct sort of object
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class CommandFactory {
    public static BinaryCommand create(ByteBuffer header) throws ProtocolException {
        header.rewind();
        if (header.get() != (byte) 0x80) {
            // create a better one... this is an illegal command
            throw new ProtocolException("Illegal magic");
        }

        CommandCode cc = CommandCode.valueOf(header.get());
        header.rewind();
        switch (cc) {
            case ADD:
            case ADDQ:
            case APPEND:
            case APPENDQ:
            case PREPEND:
            case PREPENDQ:
            case SET:
            case SETQ:
            case REPLACE:
            case REPLACEQ:
                return new BinaryStoreCommand(header);

            case INCREMENT:
            case INCREMENTQ:
            case DECREMENT:
            case DECREMENTQ:
                return new BinaryArithmeticCommand(header);

            case GET:
            case GETQ:
            case GETK:
            case GETKQ:
            case GAT:
            case GATQ:
            case TOUCH:
            case GETL:
            case GET_REPLICA:
                return new BinaryGetCommand(header);

            case OBSERVE:
                return new BinaryObserveCommand(header);

            case HELLO:
                return new BinaryHelloCommand(header);

            case OBSERVE_SEQNO:
                return new BinaryObserveSeqnoCommand(header);

            default:
                return new BinaryCommand(header);
        }
    }

    private CommandFactory() {
    }

}
