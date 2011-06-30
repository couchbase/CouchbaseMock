/**
 *     Copyright 2011 Membase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.couchbase.mock.memcached;

/**
 *
 * @author Trond Norbye
 */
public class Item {
    private String key;
    private int flags;
    private int exptime;
    private byte[] value;
    private long cas;

    Item(String key, int flags, int exptime, byte[] value, long cas) {
        this.key = key;
        this.flags = flags;
        this.exptime = exptime;
        this.value = value;
        this.cas = cas;
    }

    public int getExptime() {
        return exptime;
    }

    public int getFlags() {
        return flags;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getCas() {
        return cas;
    }

    void setCas(long l) {
        cas = l;
    }


}
