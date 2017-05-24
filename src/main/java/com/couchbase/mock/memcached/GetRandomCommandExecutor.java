/*
 * Copyright 2017 Couchbase, Inc.
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

package com.couchbase.mock.memcached;

import com.couchbase.mock.memcached.protocol.BinaryGetResponse;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.ErrorCode;
import com.couchbase.mock.memcached.protocol.BinaryCommand;

public class GetRandomCommandExecutor implements CommandExecutor {
    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        VBucketStore cache;
        Item itm = server.getStorage().getRandomItem();
        if (itm != null) {
            client.sendResponse(new BinaryGetResponse(cmd, itm));
        } else {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.KEY_ENOENT));
        }
    }
}
