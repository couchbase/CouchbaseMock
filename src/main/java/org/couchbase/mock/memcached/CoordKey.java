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

package org.couchbase.mock.memcached;

/**
 * @author Mark Nunberg
 * This class is used to lookup historical vBucket information when required
 */
public class CoordKey {
    private final int vbid;
    private final long uuid;

    public CoordKey(int vbid, long uuid) {
        this.vbid = vbid;
        this.uuid = uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CoordKey coordKey = (CoordKey) o;

        if (uuid != coordKey.uuid) return false;
        if (vbid != coordKey.vbid) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = vbid;
        result = 31 * result + (int) (uuid ^ (uuid >>> 32));
        return result;
    }
}
