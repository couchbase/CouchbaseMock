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

/**
 * @author Trond Norbye
 *
 */
public enum ErrorCode {
    SUCCESS(0x00), KEY_ENOENT(0x01), KEY_EEXISTS(0x02), E2BIG(0x03), EINVAL(
            0x04), NOT_STORED(0x05), DELTA_BADVAL(0x06), NOT_MY_VBUCKET(0x07), AUTH_ERROR(
            0x20), AUTH_CONTINUE(0x21), UNKNOWN_COMMAND(0x81), ENOMEM(0x82), NOT_SUPPORTED(
            0x83), EINTERNAL(0x84), EBUSY(0x85), ETMPFAIL(0x86);

    private final short value;

    ErrorCode(int value) {
        this.value = (short) value;
    }

    public short value() {
        return value;
    }

    static ErrorCode valueOf(short cc) {
        switch (cc) {
        case 0x00:
            return SUCCESS;
        case 0x01:
            return KEY_ENOENT;
        case 0x02:
            return KEY_EEXISTS;
        case 0x03:
            return E2BIG;
        case 0x04:
            return EINVAL;
        case 0x05:
            return NOT_STORED;
        case 0x06:
            return DELTA_BADVAL;
        case 0x07:
            return NOT_MY_VBUCKET;
        case 0x20:
            return AUTH_ERROR;
        case 0x21:
            return AUTH_CONTINUE;
        case 0x81:
            return UNKNOWN_COMMAND;
        case 0x82:
            return ENOMEM;
        case 0x83:
            return NOT_SUPPORTED;
        case 0x84:
            return EINTERNAL;
        case 0x85:
            return EBUSY;
        case 0x86:
            return ETMPFAIL;
        default:
            return UNKNOWN_COMMAND;
        }
    }
}
