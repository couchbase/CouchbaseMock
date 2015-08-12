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
package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.BinaryStatResponse;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class StatCommandExecutor implements CommandExecutor {

    private void keyStats(BinaryCommand cmd, String key, MemcachedServer server, MemcachedConnection client) {
        VBucketStore cache;
        VBucketInfo vbInfo;

        String[] kstatReq = key.split(" ");
        if (kstatReq.length != 3) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.EINVAL));
        }

        key = kstatReq[1];
        short vbid = Short.parseShort(kstatReq[2]);
        vbInfo = server.getStorage().getVBucketInfo(vbid);
        cache = server.getStorage().getCache(vbid);
        if (!vbInfo.hasAccess(server)) {
            throw new IllegalArgumentException("Wrong vBucket");
        }

        // Send the stats
        KeySpec ks = new KeySpec(key, vbid);
        Item item = cache.get(ks);
        Item diskItem =  server.getStorage().getPersisted(ks);

        if (item == null) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.KEY_ENOENT));
            return;
        }

        String stateStr = server == vbInfo.getOwner() ? "active" : "replica";
        client.sendResponse(new BinaryStatResponse(cmd, "key_vb_state", stateStr));
        client.sendResponse(new BinaryStatResponse(cmd, "key_flags", ""+item.getFlags()));
        client.sendResponse(new BinaryStatResponse(cmd, "key_cas", ""+item.getCas()));
        client.sendResponse(new BinaryStatResponse(cmd, "key_exptime", ""+item.getExpiryTime()));
        boolean isDirty = false;
        if (diskItem == null || diskItem.getCas() != item.getCas()) {
            isDirty = true;
        }
        client.sendResponse(new BinaryStatResponse(cmd, "key_is_dirty", isDirty ? "1" : "0"));
        client.sendResponse(new BinaryResponse(cmd, ErrorCode.SUCCESS));
    }

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        String key = cmd.getKey();
        if (key != null && key.startsWith("key ")) {
            try {
                keyStats(cmd, key, server, client);
            } catch (IllegalArgumentException ex) {
                client.sendResponse(new BinaryResponse(cmd, ErrorCode.EINVAL));
            }
            return;
        }

        if ("uuid".equals(key)) {
            client.sendResponse(new BinaryStatResponse(cmd, "uuid", server.getBucket().getUUID()));

        } else {
            Map<String,String> myStats = server.getStats(key);
            if (myStats == null) {
                client.sendResponse(new BinaryResponse(cmd, ErrorCode.KEY_ENOENT));
                return;
            }

            for (Entry<String, String> stat : myStats.entrySet()) {
                client.sendResponse(new BinaryStatResponse(cmd, stat.getKey(), stat.getValue()));
            }
        }
        client.sendResponse(new BinaryResponse(cmd, ErrorCode.SUCCESS));
    }
}
