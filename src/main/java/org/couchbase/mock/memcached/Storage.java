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
import java.util.HashMap;
import java.util.Map;
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
    private final VBucketInfo vbInfo[];
    private final VBucketStore cacheStore;
    private final Map<KeySpec,Item> persistStore;
    private final MemcachedServer server;

    private boolean persistEnabled = true;
    private boolean replicationEnabled = true;

    private class DeleteActionCallback implements VBucketStore.ItemAction {
        private final Storage storage;
        public DeleteActionCallback(Storage storage) {
            this.storage = storage;
        }

        @Override
        public void onAction(VBucketStore cacheStore, Item itm) {
            if (storage.persistEnabled) {
                storage.persistDeletedItem(itm.getKeySpec());
            }
            if (storage.replicationEnabled) {
                storage.replicateDeletedItem(itm.getKeySpec());
            }
        }
    }

    private class MutateActionCallback implements VBucketStore.ItemAction {
        private final Storage storage;
        public MutateActionCallback(Storage storage) {
            this.storage = storage;
        }

        @Override
        public void onAction(VBucketStore cacheStore, Item itm) {
            if (storage.persistEnabled) {
                storage.persistMutatedItem(itm);
            }
            if (storage.replicationEnabled) {
                storage.replicateMutatedItem(itm);
            }
        }
    }

    public Storage(VBucketInfo vbi[], MemcachedServer server) {
        vbInfo = vbi;
        persistStore = new HashMap<KeySpec, Item>();
        VBucketStore.ItemAction deleteCallback = new DeleteActionCallback(this);
        VBucketStore.ItemAction mutateCallback = new MutateActionCallback(this);
        cacheStore = new VBucketStore();
        cacheStore.onItemDelete = deleteCallback;
        cacheStore.onItemMutated = mutateCallback;
        this.server = server;
    }

    public void persistDeletedItem(KeySpec ks) {
        persistStore.remove(ks);
    }

    public void persistMutatedItem(Item itm) {
        persistStore.put(itm.getKeySpec(), new Item(itm));
    }

    public void replicateMutatedItem(Item itm) {
        VBucketInfo vbi = vbInfo[itm.getKeySpec().vbId];
        if (vbi.getOwner() != server) {
            return;
        }
        for (MemcachedServer replica : vbi.getReplicas()) {
            Item newItem = new Item(itm);
            VBucketStore rStore = replica.getStorage().cacheStore;
            rStore.getMap().put(newItem.getKeySpec(), newItem);
            rStore.onItemMutated.onAction(rStore, newItem);
        }
    }
    public void replicateDeletedItem(KeySpec ks) {
        VBucketInfo vbi = vbInfo[ks.vbId];
        if (vbi.getOwner() != server) {
            return;
        }
        Item itm = new Item(ks);
        for (MemcachedServer replica : vbi.getReplicas()) {
            VBucketStore rStore = replica.getStorage().cacheStore;
            rStore.getMap().remove(ks);
            rStore.onItemDelete.onAction(rStore, itm);
            rStore.onItemMutated.onAction(rStore, itm);
        }
    }

    public void setPersistenceEnabled(boolean val) {
        persistEnabled = val;
    }

    public void setReplicationEnabled(boolean val) {
        replicationEnabled = val;
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
        persistStore.put(itm.getKeySpec(), itm);
    }
    public void removeCached(KeySpec ks) {
        cacheStore.getMap().remove(ks);
    }
    public void removePersisted(KeySpec ks) {
        persistStore.remove(ks);
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

    public void flush() {
        cacheStore.getMap().clear();
        persistStore.clear();
    }
}
