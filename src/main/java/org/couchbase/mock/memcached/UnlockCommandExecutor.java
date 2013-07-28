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

import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.ErrorCode;

public class UnlockCommandExecutor implements CommandExecutor {
    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        VBucketStore cache = server.getCache(cmd);
        Item item = cache.get(cmd.getKeySpec());
        ErrorCode ec;

        if (item == null) {
            ec = ErrorCode.KEY_ENOENT;
        } else if (!item.isLocked()) {
            ec = ErrorCode.ETMPFAIL;
        } else if (!item.ensureUnlocked(cmd.getCas())) {
            ec = ErrorCode.ETMPFAIL;
        } else {
            ec = ErrorCode.SUCCESS;
        }

        client.sendResponse(new BinaryResponse(cmd, ec));
    }
}
