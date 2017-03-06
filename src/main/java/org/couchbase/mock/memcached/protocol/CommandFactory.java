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
            throw new ProtocolException("Illegal magic: " + header.get(0));
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

            case SUBDOC_EXISTS:
            case SUBDOC_GET:
            case SUBDOC_GET_COUNT:
            case SUBDOC_COUNTER:
            case SUBDOC_ARRAY_ADD_UNIQUE:
            case SUBDOC_ARRAY_INSERT:
            case SUBDOC_ARRAY_PUSH_FIRST:
            case SUBDOC_ARRAY_PUSH_LAST:
            case SUBDOC_DELETE:
            case SUBDOC_REPLACE:
            case SUBDOC_DICT_ADD:
            case SUBDOC_DICT_UPSERT:
                return new BinarySubdocCommand(header);
            case SUBDOC_MULTI_LOOKUP:
                return new BinarySubdocMultiLookupCommand(header);
            case SUBDOC_MULTI_MUTATION:
                return new BinarySubdocMultiMutationCommand(header);
            case GET_ERRMAP:
                return new BinaryGetErrmapCommand(header);
            case SELECT_BUCKET:
                return new BinarySelectBucketCommand(header);

            default:
                return new BinaryCommand(header);
        }
    }

    private CommandFactory() {
    }

}
