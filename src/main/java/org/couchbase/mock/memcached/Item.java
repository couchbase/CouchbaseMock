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

import java.util.Date;

/**
 * @author Trond Norbye
 */
public class Item {
    private final String key;
    private final int flags;
    private int expiryTime;
    private byte[] value;
    private long cas;
    private long modificationTime;

    /** When the lock expires, if any */
    private int lockExpiryTime;

    public Item(String key, int flags, int expiryTime, byte[] value, long cas) {
        this.key = key;
        this.flags = flags;
        this.expiryTime = expiryTime;
        this.value = value;
        this.cas = cas;
    }

    public int getExpiryTime() {
        return expiryTime;
    }

    public long getExpiryTimeInMillis() {
        return (long) expiryTime * 1000L;
    }

    public void setExpiryTime(int e) {
        expiryTime = DataStore.convertExptime(e);
    }

    public long getModificationTime() {
        return modificationTime;
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
        modificationTime = new Date().getTime();
        cas = l;
    }

    void setLockExpiryTime(int e) {
        lockExpiryTime = DataStore.convertExptime(e);
    }

    int getLockExpiryTime() {
        return lockExpiryTime;
    }

    public boolean isLocked() {
        if (lockExpiryTime == 0) {
            return false;
        }

        long now = new Date().getTime() / 1000;
        if (now > lockExpiryTime) {
            return false;
        }
        return true;
    }

    /**
     * Given a cas, ensure that the item is unlocked.
     * Will succeed if cas matches the existing cas, or if the item is not
     * locked
     * @param cas
     * @return true if item is *not* locked.
     */
    public boolean ensureUnlocked(long cas)
    {
        if (cas == this.cas) {
            lockExpiryTime = 0;
            return true;
        } else {
            return isLocked() == false;
        }
    }

    public void append(Item i) {
        byte[] s1 = value;
        byte[] s2 = i.getValue();
        byte[] dst = new byte[s1.length + s2.length];

        System.arraycopy(s1, 0, dst, 0, s1.length);
        System.arraycopy(s2, 0, dst, s1.length, s2.length);
        value = dst;
    }

    public void prepend(Item i) {
        byte[] s1 = value;
        byte[] s2 = i.getValue();
        byte[] dst = new byte[s1.length + s2.length];

        System.arraycopy(s2, 0, dst, 0, s2.length);
        System.arraycopy(s1, 0, dst, s2.length, s1.length);
        value = dst;
    }
}
