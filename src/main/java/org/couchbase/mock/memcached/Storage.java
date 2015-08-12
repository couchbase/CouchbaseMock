/*
 * Copyright 2013 Couchbase, Inc.
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

import java.security.AccessControlException;
import java.util.*;

import org.couchbase.mock.Bucket;

/**
 * Class representing a node's storage.
 *
 * This has both a "cache" and a "persistent" store, with the aim to support
 * full replication and persistence semantics.
 *
 * The "cache" store receives items (either via a set or a delete). Once this
 * is done, the replication and/or persistence hooks are invoked.
 *
 * The persistent store keeps a copy of each Item (when persisted), and
 * each server's "Replica" keeps a new copy of the same Item as well. While this
 * is probably not the most efficient way to go about things, it is crucial in
 * order to be able to test these types of semantics.
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class Storage {
    public enum StorageType { CACHE, DISK }
    private final VBucketInfo vbInfo[];
    private final VBucketStore cacheStore;
    private final PersistentStorage persistStore;
    private final MemcachedServer server;
    private boolean persistEnabled = true;
    private boolean replicationEnabled = true;

    private final static VBucketCoordinates EMPTY_COORDS = new BasicVBucketCoordinates(0, 0);
    private class PersistentStorage {
        class Slot {
            long uuid = 0;
            long seqno = 0;
            Map<KeySpec, Item> mm = new HashMap<KeySpec, Item>();
        }
        final Slot[] slots;

        PersistentStorage(int nvb) {
            slots = new Slot[nvb];
        }

        private Slot updateCommon(KeySpec ks, VBucketCoordinates coords) {
            Slot slot = slots[ks.vbId];
            if (slot == null) {
                slot = slots[ks.vbId] = new Slot();
            }
            if (coords.getUuid() != 0 && coords.getSeqno() != 0) {
                slot.uuid = coords.getUuid();
                slot.seqno = coords.getSeqno();
            }
            return slot;
        }

        public void put(Item item, VBucketCoordinates coords) {
            updateCommon(item.getKeySpec(), coords).mm.put(item.getKeySpec(), item);
        }

        public Item get(KeySpec ks) {
            Slot ss = slots[ks.vbId];
            if (ss != null) {
                return ss.mm.get(ks);
            } else {
                return null;
            }
        }

        public Collection<Item> values() {
            ArrayList<Item> ret = new ArrayList<Item>();
            for (Slot s : slots) {
                if (s != null) {
                    ret.addAll(s.mm.values());
                }
            }
            return ret;
        }

        public void clear() {
            for (Slot s : slots) {
                if (s != null) {
                    s.mm.clear();
                }
            }
        }

        public void remove(KeySpec ks, VBucketCoordinates coords) {
            updateCommon(ks, coords).mm.remove(ks);
        }

        VBucketCoordinates getCoords(int vbid) {
            Slot ss = slots[vbid];
            long seqno = 0;
            long uuid = 0;
            if (ss != null) {
                seqno = ss.seqno;
                uuid = ss.uuid;
            }

            return new BasicVBucketCoordinates(uuid, seqno);
        }

        public void updateSingleCoords(int vbid, VBucketCoordinates coords) {
            Slot s = slots[vbid];
            if (s != null) {
                s.uuid = coords.getUuid();
                s.seqno = coords.getSeqno();
            }
        }
    }

    private class DeleteActionCallback implements VBucketStore.ItemAction {
        private final Storage storage;
        public DeleteActionCallback(Storage storage) {
            this.storage = storage;
        }

        @Override
        public void onAction(VBucketStore cacheStore, Item itm, VBucketCoordinates coords) {
            if (storage.persistEnabled) {
                storage.persistDeletedItem(itm.getKeySpec(), coords);
            }
            if (storage.replicationEnabled) {
                storage.replicateDeletedItem(itm.getKeySpec(), coords);
            }
        }
    }

    private class MutateActionCallback implements VBucketStore.ItemAction {
        private final Storage storage;
        public MutateActionCallback(Storage storage) {
            this.storage = storage;
        }

        @Override
        public void onAction(VBucketStore cacheStore, Item itm, VBucketCoordinates coords) {
            if (storage.persistEnabled) {
                storage.persistMutatedItem(itm, coords);
            }
            if (storage.replicationEnabled) {
                storage.replicateMutatedItem(itm, coords);
            }
        }
    }

    public Storage(VBucketInfo vbi[], MemcachedServer server) {
        vbInfo = vbi;
        VBucketStore.ItemAction deleteCallback = new DeleteActionCallback(this);
        VBucketStore.ItemAction mutateCallback = new MutateActionCallback(this);
        cacheStore = new VBucketStore(vbi);
        persistStore = new PersistentStorage(vbi.length);
        cacheStore.onItemDelete = deleteCallback;
        cacheStore.onItemMutated = mutateCallback;
        this.server = server;
    }

    public void persistDeletedItem(KeySpec ks, VBucketCoordinates coords) {
        persistStore.remove(ks, coords);
    }

    public void persistMutatedItem(Item itm, VBucketCoordinates coords) {
        persistStore.put(new Item(itm), coords);
    }

    private void replicateMutatedItem(Item itm, VBucketCoordinates coords) {
        VBucketInfo vbi = vbInfo[itm.getKeySpec().vbId];
        if (vbi.getOwner() != server) {
            return;
        }
        for (MemcachedServer replica : vbi.getReplicas()) {
            Item newItem = new Item(itm);
            VBucketStore rStore = replica.getStorage().cacheStore;
            rStore.forceStorageMutation(newItem, coords);
        }
    }

    private void replicateDeletedItem(KeySpec ks, VBucketCoordinates coords) {
        VBucketInfo vbi = vbInfo[ks.vbId];
        if (vbi.getOwner() != server) {
            return;
        }
        Item itm = new Item(ks);
        for (MemcachedServer replica : vbi.getReplicas()) {
            VBucketStore rStore = replica.getStorage().cacheStore;
            PersistentStorage pStore = replica.getStorage().persistStore;
            rStore.forceDeleteMutation(itm, coords);

            // Nasty hack needed to retain compat with existing tests which assume that
            // deletion operations on the mock will silently 'persist' this mutation
            // on disk.
            pStore.put(itm, coords);
        }
    }

    public Item getCached(KeySpec ks) {
        return cacheStore.get(ks);
    }
    public Item getPersisted(KeySpec ks) {
        return persistStore.get(ks);
    }
    public void putCached(Item itm) {
        cacheStore.getMap().put(itm.getKeySpec(), itm);
    }
    public void putPersisted(Item itm) {
        persistStore.put(itm, EMPTY_COORDS);
    }
    public void removeCached(KeySpec ks) {
        cacheStore.getMap().remove(ks);
    }
    public void removePersisted(KeySpec ks) {
        persistStore.remove(ks, EMPTY_COORDS);
    }

    public VBucketInfo getVBucketInfo(short vb) {
        if (vb < 0 || vb > vbInfo.length) {
            throw new AccessControlException("Invalid vBucket");
        }
        return vbInfo[vb];
    }

    private void verifyOwnership(MemcachedServer server, short vBucketId) {
        if (server != null && server.getBucket().getType() == Bucket.BucketType.MEMCACHED) {
            return;
        }
        if (vBucketId < 0 || vBucketId > vbInfo.length) {
            throw new AccessControlException("Invalid vBucket");
        }
        VBucketInfo vbi = vbInfo[vBucketId];
        if (server != null && vbi.getOwner() != server) {
            throw new AccessControlException("Server is not master for this vb");
        }
    }

    public VBucketStore getCache(MemcachedServer server, short vBucketId) {
        verifyOwnership(server, vBucketId);
        return cacheStore;
    }

    public VBucketStore getCache(short vBucketId) {
        verifyOwnership(null, vBucketId);
        return cacheStore;
    }

    public long getPersistedSeqno(short vBucketId) {
        return persistStore.getCoords(vBucketId).getSeqno();
    }

    public Iterable<Item> getMasterStore(final StorageType type) {
        // Create the list now:
        List<Item> validItems = new ArrayList<Item>();
        Collection<Item> inputs;

        if (type == StorageType.CACHE) {
            inputs = cacheStore.getMap().values();
        } else {
            inputs = persistStore.values();
        }

        for (Item itm : inputs) {
            int vbId = itm.getKeySpec().vbId;
            MemcachedServer owner = vbInfo[vbId].getOwner();
            if (owner == server) {
                validItems.add(itm);
            }
        }

        return validItems;
    }

    public void flush() {
        cacheStore.getMap().clear();
        persistStore.clear();
    }

    public void updateCoordinateInfo(VBucketInfo[] vbi) {
        cacheStore.updateCoords(vbi);
        for (int i = 0; i < vbi.length; i++) {
            persistStore.updateSingleCoords(i, cacheStore.getCurrentCoords(i));
        }
    }
}
