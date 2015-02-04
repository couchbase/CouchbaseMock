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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.couchbase.mock.Info;
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
        public void onAction(VBucketStore store, Item itm, VBucketCoordinates ms);
    }

    private volatile long casCounter;
    private static final long THIRTY_DAYS = 30 * 24 * 60 * 60;
    static final int DEFAULT_EXPIRY_TIME = 15;
    static final int MAXIMUM_EXPIRY_TIME = 29;

    private final Map<KeySpec, Item> kv = new ConcurrentHashMap<KeySpec, Item>();
    private final StorageVBucketCoordinates[] vbCoords;
    private final Map<CoordKey, VBucketCoordinates>allCoords = new HashMap<CoordKey, VBucketCoordinates>();

    public ItemAction onItemDelete;
    public ItemAction onItemMutated;

    public VBucketStore(VBucketInfo[] vbi) {
        vbCoords = new StorageVBucketCoordinates[vbi.length];
        setCurrentCoords(vbi);
    }

    private void logCoords(int vbid, VBucketCoordinates coords) {
        CoordKey key = new CoordKey(vbid, coords.getUuid());
        allCoords.put(key, coords);
    }

    public VBucketCoordinates findCoords(int vbid, long uuid) {
        return allCoords.get(new CoordKey(vbid, uuid));
    }

    private void setCurrentCoords(VBucketInfo[] vbi) {
        for (int i = 0; i < vbi.length; i++) {
            StorageVBucketCoordinates curCoords = new StorageVBucketCoordinates(vbi[i].getUuid());
            vbCoords[i] = curCoords;
            logCoords(i, curCoords);
        }
    }

    void updateCoords(VBucketInfo[] vbi) {
        for (int i = 0; i < vbCoords.length; i++) {
            logCoords(i, vbCoords[i]);
        }
        synchronized (vbCoords) {
            setCurrentCoords(vbi);
        }
    }

    public VBucketCoordinates getCurrentCoords(int vbid) {
        return vbCoords[vbid];
    }

    /**
     * Increments the current coordinates for a new mutation.
     * @param ks The key spec containing the vBucket ID whose coordinates should be increases
     * @return A status object.
     */
    private MutationStatus incrCoords(KeySpec ks) {
        final StorageVBucketCoordinates curCoord;
        synchronized (vbCoords) {
            curCoord = vbCoords[ks.vbId];
        }

        long seq = curCoord.incrSeqno();
        long uuid = curCoord.getUuid();
        VBucketCoordinates coord = new BasicVBucketCoordinates(uuid, seq);
        return new MutationStatus(coord);
    }

    private Item lookup(KeySpec ks) {
        Item ii = kv.get(ks);
        if (ii == null) {
            return null;
        }

        long now = new Date().getTime() + Info.getClockOffset() * 1000L;
        if (ii.getExpiryTime() == 0 || now < ii.getExpiryTimeInMillis()) {
            return ii;
        }
        MutationStatus ms = incrCoords(ii.getKeySpec());
        onItemDelete.onAction(this, ii, ms.getCoords());
        kv.remove(ks);
        return null;
    }

    public ErrorCode lock(Item item, int expiry) {
        if (item.isLocked()) {
            return ErrorCode.ETMPFAIL;

        } else {
            if (expiry == 0 || expiry > MAXIMUM_EXPIRY_TIME) {
                expiry = DEFAULT_EXPIRY_TIME;
            }
            MutationStatus ms = incrCoords(item.getKeySpec());
            item.setLockExpiryTime(expiry);
            onItemMutated.onAction(this, item, ms.getCoords());
            return ErrorCode.SUCCESS;
        }
    }

    public ErrorCode touch(Item item, int expiry) {
        item.setExpiryTime(expiry);
        MutationStatus ms = incrCoords(item.getKeySpec());
        onItemMutated.onAction(this, item, ms.getCoords());
        return ErrorCode.SUCCESS;
    }

    public MutationStatus add(Item item) {
        // I don't give a shit about atomicity right now..
        Item old = lookup(item.getKeySpec());
        if (old != null || item.getCas() != 0) {
            return new MutationStatus(ErrorCode.KEY_EEXISTS);
        }

        item.setCas(++casCounter);
        kv.put(item.getKeySpec(), item);
        MutationStatus ms = incrCoords(item.getKeySpec());
        onItemMutated.onAction(this, item, ms.getCoords());
        return ms;
    }

    public MutationStatus replace(Item item) {
        // I don't give a shit about atomicity right now..
        Item old = lookup(item.getKeySpec());
        if (old == null) {
            return new MutationStatus(ErrorCode.KEY_ENOENT);
        }

        if (item.getCas() != old.getCas()) {
            if (item.getCas() != 0) {
                return new MutationStatus(ErrorCode.KEY_EEXISTS);
            }
        }

        if (!old.ensureUnlocked(item.getCas())) {
            return new MutationStatus(ErrorCode.KEY_EEXISTS);
        }

        MutationStatus ms = incrCoords(item.getKeySpec());
        item.setCas(++casCounter);
        kv.put(item.getKeySpec(), item);
        onItemMutated.onAction(this, item, ms.getCoords());
        return ms;
    }

    public MutationStatus set(Item item) {
        if (item.getCas() == 0) {
            Item old = lookup(item.getKeySpec());
            if (old != null && old.isLocked()) {
                return new MutationStatus(ErrorCode.KEY_EEXISTS);
            }

            MutationStatus ms = incrCoords(item.getKeySpec());
            item.setCas(++casCounter);
            kv.put(item.getKeySpec(), item);
            onItemMutated.onAction(this, item, ms.getCoords());
            return ms;
        } else {
            return replace(item);
        }
    }

    public MutationStatus delete(KeySpec ks, long cas) {
        // I don't give a shit about atomicity right now..
        Item i = lookup(ks);
        if (i == null) {
            return new MutationStatus(ErrorCode.KEY_ENOENT);
        }

        if (!i.ensureUnlocked(cas)) {
            return new MutationStatus(ErrorCode.ETMPFAIL);
        }

        if (cas == 0 || cas == i.getCas()) {
            MutationStatus ms = incrCoords(i.getKeySpec());
            kv.remove(ks);
            onItemDelete.onAction(this, i, ms.getCoords());
            return ms;
        }
        return new MutationStatus(ErrorCode.KEY_EEXISTS);
    }

    private MutationStatus modifyItemValue(Item i, boolean isAppend) {
        Item old = lookup(i.getKeySpec());
        if (old == null) {
            return new MutationStatus(ErrorCode.KEY_ENOENT);
        }
        if (!old.ensureUnlocked(i.getCas())) {
            return new MutationStatus(ErrorCode.KEY_EEXISTS);
        }
        if (isAppend) {
            old.append(i);
        } else {
            old.prepend(i);
        }
        MutationStatus ms = incrCoords(old.getKeySpec());
        old.setCas(++casCounter);
        onItemMutated.onAction(this, old, ms.getCoords());
        return ms;
    }

    public MutationStatus append(Item i) {
        return modifyItemValue(i, true);
    }

    public MutationStatus prepend(Item i) {
        return modifyItemValue(i, false);
    }

    public Item get(KeySpec ks) {
        return lookup(ks);
    }

    private void forceMutation(int vbid, Item itm, VBucketCoordinates coords, boolean isDelete) {
        StorageVBucketCoordinates cur;
        synchronized (vbCoords) {
            cur  = vbCoords[vbid];
            if (cur.getUuid() != coords.getUuid()) {
                cur = vbCoords[vbid] = new StorageVBucketCoordinates(coords);
            }
        }
        cur.seekSeqno(coords.getSeqno());
        if (isDelete) {
            kv.remove(itm.getKeySpec());
            onItemDelete.onAction(this, itm, coords);
        } else {
            kv.put(itm.getKeySpec(), itm);
            onItemMutated.onAction(this, itm, coords);
        }
    }

    /**
     * Force a storage of an item to the cache.
     *
     * This assumes the current object belongs to a replica, as it will blindly
     * assume information passed here is authoritative.
     *
     * @param itm The item to mutate (should be a copy of the original)
     * @param coords Coordinate info
     */
    void forceStorageMutation(Item itm, VBucketCoordinates coords) {
        forceMutation(itm.getKeySpec().vbId, itm, coords, false);
    }

    /**
     * Forces the deletion of an item from the case.
     *
     * @see #forceStorageMutation(Item, VBucketCoordinates)
     * @param itm
     * @param coords
     */
    void forceDeleteMutation(Item itm, VBucketCoordinates coords) {
        forceMutation(itm.getKeySpec().vbId, itm, coords, true);
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
        if (original == 0) {
            return original;
        } else if (original > THIRTY_DAYS) {
            return original + (int)Info.getClockOffset();
        }

        return (int)((new Date().getTime() / 1000) + original + Info.getClockOffset());
    }


}
