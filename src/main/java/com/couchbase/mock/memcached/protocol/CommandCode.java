/*
 * Copyright 2017 Couchbase, Inc.
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
package com.couchbase.mock.memcached.protocol;

/**
 * @author Trond Norbye
 */
@SuppressWarnings({"SpellCheckingInspection", "UnusedDeclaration"})
public enum CommandCode {
    GET(0x00), SET(0x01), ADD(0x02), REPLACE(0x03),
    DELETE(0x04), INCREMENT(0x05), DECREMENT(0x06), QUIT(0x07),
    FLUSH(0x08), GETQ(0x09), NOOP(0x0a), VERSION(0x0b),
    GETK(0x0c), GETKQ(0x0d), APPEND(0x0e), PREPEND(0x0f),
    STAT(0x10), SETQ(0x11), ADDQ(0x12), REPLACEQ(0x13),
    DELETEQ(0x14), INCREMENTQ(0x15), DECREMENTQ(0x16), QUITQ(0x17),
    FLUSHQ(0x18), APPENDQ(0x19), PREPENDQ(0x1a), VERBOSITY(0x1b),
    TOUCH(0x1c), GAT(0x1d), GATQ(0x1e), SASL_LIST_MECHS(0x20),
    SASL_AUTH(0x21), SASL_STEP(0x22), RGET(0x30), RSET(0x31),
    RSETQ(0x32), RAPPEND(0x33), RAPPENDQ(0x34), RPREPEND(0x35),
    RPREPENDQ(0x36), RDELETE(0x37), RDELETEQ(0x38), RINCR(0x39),
    RINCRQ(0x3a), RDECR(0x3b), RDECRQ(0x3c), SET_VBUCKET(0x3d),
    GET_VBUCKET(0x3e), DEL_VBUCKET(0x3f), TAP_CONNECT(0x40), TAP_MUTATION(0x41),
    TAP_DELETE(0x42), TAP_FLUSH(0x43), TAP_OPAQUE(0x44), TAP_VBUCKET_SET(0x45),
    LAST_RESERVED(0xef), SCRUB(0xf0),
    GET_REPLICA(0x83),
    SELECT_BUCKET(0x89),
    OBSERVE(0x92), EVICT(0x93),
    GETL(0x94), UNL(0x95),
    GET_CLUSTER_CONFIG(0xb5), HELLO(0x1f), GET_ERRMAP(0xfe), ILLEGAL(0xff),
    OBSERVE_SEQNO(0x91),

    GET_RANDOM(0xb6),

    // Subdoc
    SUBDOC_GET(0xC5), SUBDOC_EXISTS(0xC6),
    SUBDOC_DICT_ADD(0xC7), SUBDOC_DICT_UPSERT(0xC8),
    SUBDOC_DELETE(0xC9), SUBDOC_REPLACE(0xCA),
    SUBDOC_ARRAY_PUSH_LAST(0xCB), SUBDOC_ARRAY_PUSH_FIRST(0xCC),
    SUBDOC_ARRAY_INSERT(0xCD), SUBDOC_ARRAY_ADD_UNIQUE(0xCE),
    SUBDOC_COUNTER(0xCF),
    SUBDOC_MULTI_LOOKUP(0xD0), SUBDOC_MULTI_MUTATION(0xD1),
    SUBDOC_GET_COUNT(0xD2);



    private final int value;

    CommandCode(int value) {
        this.value = value;
    }

    public int cc() {
        return value;
    }

    public static CommandCode valueOf(byte cc) {
        return valueOf((int)cc & 0xff);
    }

    public static CommandCode valueOf(int cc) {
        switch (cc) {
            case 0x00:
                return GET;
            case 0x01:
                return SET;
            case 0x02:
                return ADD;
            case 0x03:
                return REPLACE;
            case 0x04:
                return DELETE;
            case 0x05:
                return INCREMENT;
            case 0x06:
                return DECREMENT;
            case 0x07:
                return QUIT;
            case 0x08:
                return FLUSH;
            case 0x09:
                return GETQ;
            case 0x0a:
                return NOOP;
            case 0x0b:
                return VERSION;
            case 0x0c:
                return GETK;
            case 0x0d:
                return GETKQ;
            case 0x0e:
                return APPEND;
            case 0x0f:
                return PREPEND;
            case 0x10:
                return STAT;
            case 0x11:
                return SETQ;
            case 0x12:
                return ADDQ;
            case 0x13:
                return REPLACEQ;
            case 0x14:
                return DELETEQ;
            case 0x15:
                return INCREMENTQ;
            case 0x16:
                return DECREMENTQ;
            case 0x17:
                return QUITQ;
            case 0x18:
                return FLUSHQ;
            case 0x19:
                return APPENDQ;
            case 0x1a:
                return PREPENDQ;
            case 0x1b:
                return VERBOSITY;
            case 0x1c:
                return TOUCH;
            case 0x1d:
                return GAT;
            case 0x1e:
                return GATQ;
            case 0x20:
                return SASL_LIST_MECHS;
            case 0x21:
                return SASL_AUTH;
            case 0x22:
                return SASL_STEP;
            case 0x93:
                return EVICT;
            case 0x94:
                return GETL;
            case 0x95:
                return UNL;
            case 0x83:
                return GET_REPLICA;
            case 0x92:
                return OBSERVE;
            case 0xb5:
                return GET_CLUSTER_CONFIG;
            case 0x1f:
                return HELLO;
            case 0x91:
                return OBSERVE_SEQNO;
            case 0xc5:
                return SUBDOC_GET;
            case 0xc6:
                return SUBDOC_EXISTS;
            case 0xc7:
                return SUBDOC_DICT_ADD;
            case 0xc8:
                return SUBDOC_DICT_UPSERT;
            case 0xc9:
                return SUBDOC_DELETE;
            case 0xca:
                return SUBDOC_REPLACE;
            case 0xcb:
                return SUBDOC_ARRAY_PUSH_LAST;
            case 0xcc:
                return SUBDOC_ARRAY_PUSH_FIRST;
            case 0xcd:
                return SUBDOC_ARRAY_INSERT;
            case 0xce:
                return SUBDOC_ARRAY_ADD_UNIQUE;
            case 0xcf:
                return SUBDOC_COUNTER;
            case 0xd0:
                return SUBDOC_MULTI_LOOKUP;
            case 0xd1:
                return SUBDOC_MULTI_MUTATION;
            case 0xd2:
                return SUBDOC_GET_COUNT;
            case 0xb6:
                return GET_RANDOM;
            case 0xfe:
                return GET_ERRMAP;
            case 0x89:
                return SELECT_BUCKET;
            default:
                return ILLEGAL;
        }
    }

    static String toString(CommandCode cc) {
        switch (cc) {
            case SET:
                return "set";
            case ADD:
                return "add";
            case REPLACE:
                return "replace";
            case FLUSH:
                return "flush";
            case DELETE:
                return "delete";
            case INCREMENT:
                return "increment";
            case DECREMENT:
                return "decrement";
            case PREPEND:
                return "prepend";
            case APPEND:
                return "append";
            case VERSION:
                return "version";
            case NOOP:
                return "noop";
            case QUIT:
                return "quit";
            case GET:
                return "get";
            case GETQ:
                return "getq";
            case GETK:
                return "getk";
            case GETKQ:
                return "getkq";
            case ILLEGAL:
                return "Illegal";
            case STAT:
                return "stat";
            case SETQ:
                return "setq";
            case ADDQ:
                return "addq";
            case REPLACEQ:
                return "replaceq";
            case DELETEQ:
                return "deleteq";
            case INCREMENTQ:
                return "incrementq";
            case DECREMENTQ:
                return "decrementq";
            case QUITQ:
                return "quitq";
            case FLUSHQ:
                return "flushq";
            case APPENDQ:
                return "appendq";
            case PREPENDQ:
                return "prependq";
            case VERBOSITY:
                return "verbosity";
            case SASL_LIST_MECHS:
                return "sasl_list_mechs";
            case SASL_AUTH:
                return "sasl_auth";
            case SASL_STEP:
                return "sasl_step";
            case TOUCH:
                return "touch";
            case GAT:
                return "gat";
            case GATQ:
                return "gatq";
            case EVICT:
                return "evict";
            case GETL:
                return "getl";
            case UNL:
                return "unl";
            case GET_REPLICA:
                return "get_replica";
            case OBSERVE:
                return "observe";
            case GET_CLUSTER_CONFIG:
                return "get_cluster_config";
            case HELLO:
                return "hello";
            case OBSERVE_SEQNO:
                return "observe_seqno";
            case SUBDOC_GET:
                return "subdoc_get";
            case SUBDOC_EXISTS:
                return "subdoc_exists";
            case SUBDOC_GET_COUNT:
                return "subdoc_get_count";
            case GET_RANDOM:
                return "get_random";
            case GET_ERRMAP:
                return "get_errormap";
            case SELECT_BUCKET:
                return "select_bucket";
            default:
                return "unknown";

        }
    }
}
