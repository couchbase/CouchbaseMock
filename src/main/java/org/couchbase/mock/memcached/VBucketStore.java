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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Storage operations representing a single vBucket. This is a replacement for
 * the old DataStore class. Specifically, it does not check for vBucket
 * ownership - that information is handled at the protocol layer.
 *
 * @author mnunberg
 */
public class VBucketStore {
    public interface ItemAction {
        public void onAction(VBucketStore store, Item itm);
    }

    private volatile long casCounter;
    private static final long THIRTY_DAYS = 30 * 24 * 60 * 60;
    private final Map<KeySpec, Item> kv = new ConcurrentHashMap<KeySpec, Item>();
    public ItemAction onItemDelete;
    public ItemAction onItemMutated;

    private Item lookup(KeySpec ks) {
        Item ii = kv.get(ks);
        if (ii == null) {
            return null;
        }

        long now = new Date().getTime();
        if (ii.getExpiryTime() == 0 || now < ii.getExpiryTimeInMillis()) {
            return ii;

        }
        onItemDelete.onAction(this, ii);
        kv.remove(ks);
        return null;
    }

    public ErrorCode add(Item item) {
        // I don't give a shit about atomicity right now..
        Item old = lookup(item.getKeySpec());
        if (old != null || item.getCas() != 0) {
            return ErrorCode.KEY_EEXISTS;
        }

        item.setCas(++casCounter);
        kv.put(item.getKeySpec(), item);
        onItemMutated.onAction(this, item);
        return ErrorCode.SUCCESS;
    }

    public ErrorCode replace(Item item) {
        // I don't give a shit about atomicity right now..
        Item old = lookup(item.getKeySpec());
        if (old == null) {
            return ErrorCode.KEY_ENOENT;
        }

        if (item.getCas() != old.getCas()) {
            if (item.getCas() != 0) {
                return ErrorCode.KEY_EEXISTS;
            }
        }

        if (!old.ensureUnlocked(item.getCas())) {
            return ErrorCode.KEY_EEXISTS;
        }

        item.setCas(++casCounter);
        kv.put(item.getKeySpec(), item);
        onItemMutated.onAction(this, item);

        return ErrorCode.SUCCESS;
    }

    public ErrorCode set(Item item) {
        if (item.getCas() == 0) {
            Item old = lookup(item.getKeySpec());
            if (old != null && old.isLocked()) {
                return ErrorCode.KEY_EEXISTS;
            }

            item.setCas(++casCounter);
            kv.put(item.getKeySpec(), item);
            onItemMutated.onAction(this, item);

            return ErrorCode.SUCCESS;
        } else {
            return replace(item);
        }
    }

    public ErrorCode delete(KeySpec ks, long cas) {
        // I don't give a shit about atomicity right now..
        Item i = lookup(ks);
        if (i == null) {
            return ErrorCode.KEY_ENOENT;
        }

        if (!i.ensureUnlocked(cas)) {
            return ErrorCode.ETMPFAIL;
        }

        if (cas == 0 || cas == i.getCas()) {
            kv.remove(ks);
            onItemDelete.onAction(this, i);
            return ErrorCode.SUCCESS;
        }
        return ErrorCode.KEY_EEXISTS;
    }

    private ErrorCode modifyItemValue(Item i, boolean isAppend) {
        Item old = lookup(i.getKeySpec());
        if (old == null) {
            return ErrorCode.KEY_ENOENT;
        }
        if (!old.ensureUnlocked(i.getCas())) {
            return ErrorCode.KEY_EEXISTS;
        }
        if (isAppend) {
            old.append(i);
        } else {
            old.prepend(i);
        }
        old.setCas(++casCounter);
        onItemMutated.onAction(this, old);
        return ErrorCode.SUCCESS;
    }

    public ErrorCode append(Item i) {
        return modifyItemValue(i, true);
    }

    public ErrorCode prepend(Item i) {
        return modifyItemValue(i, false);
    }

    public Item get(KeySpec ks) {
        return lookup(ks);
    }

    public Map<KeySpec,Item> getMap() {
        return kv;
    }

    /**
     * Converts an expiration value to an absolute Unix timestamp.
     * @param original The original value passed in from the client. This can
     *  be a relative or absolute (unix) timestamp
     * @return The converted value
     */
    public static int convertExpiryTime(int original) {
        if (original == 0 || original > THIRTY_DAYS) {
            return original;
        }

        return (int)((new Date().getTime() / 1000) + original);
    }


}
