/*
 * Copyright 2013 Couchbase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.memcached;

/**
 * This class is used as a key for our Items.
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class KeySpec {
    public final short vbId;
    public final String key;
    public KeySpec(String key, short vbId) {
        this.key = key;
        this.vbId = vbId;
    }

    @Override
    public final boolean equals(Object other) {
        if (other == null) {
            return false;
        }

        if (other == this) {
            return true;
        }

        if (KeySpec.class.isInstance(other)) {
            KeySpec ksOther = (KeySpec)other;
            return ksOther.vbId == vbId && ksOther.key.equals(key);
        }
        return false;
    }

    @Override
    public final int hashCode() {
        int hash = 5;
        hash = 19 * hash + this.vbId;
        hash = 19 * hash + ( this.key != null ? this.key.hashCode() : 0 );
        return hash;
    }
}
