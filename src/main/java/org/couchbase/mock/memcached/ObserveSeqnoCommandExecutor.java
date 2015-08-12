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

import org.couchbase.mock.memcached.protocol.*;

/**
 * Created by mnunberg on 2/4/15.
 */
public class ObserveSeqnoCommandExecutor implements CommandExecutor {
    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        Storage ss = server.getStorage();

        BinaryObserveSeqnoCommand ocmd = (BinaryObserveSeqnoCommand)cmd;
        VBucketStore cacheStore = ss.getCache(ocmd.getVBucketId());

        // Coordinates for the request
        VBucketCoordinates coordRequest = cacheStore.findCoords(ocmd.getVBucketId(), ocmd.getUuid());
        // Most recent coordinates
        VBucketCoordinates coordCurr = cacheStore.getCurrentCoords(ocmd.getVBucketId());

        if (coordRequest == null) {
            // No such coordinates!
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.EINTERNAL));
            return;
        }

        long seqnoDisk = ss.getPersistedSeqno(cmd.getVBucketId());
        long seqnoCache = coordRequest.getSeqno();

        if (coordRequest.getUuid() != coordCurr.getUuid()) {
            // Failover:
            client.sendResponse(new BinaryObserveSeqnoResponse(ocmd, coordCurr, coordRequest, seqnoDisk));
        } else {
            client.sendResponse(new BinaryObserveSeqnoResponse(ocmd, seqnoCache, seqnoDisk));
        }
    }
}
