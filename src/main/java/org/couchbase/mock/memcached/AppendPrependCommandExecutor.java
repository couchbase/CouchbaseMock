/*
 * Copyright 2011 Couchbase, Inc.
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
import org.couchbase.mock.memcached.protocol.BinaryStoreCommand;
import org.couchbase.mock.memcached.protocol.BinaryStoreResponse;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Implementation of the APPEND[Q] and PREPEND[Q] commands
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
class AppendPrependCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        BinaryStoreCommand command = (BinaryStoreCommand) cmd;
        VBucketStore cache = server.getStorage().getCache(server, cmd.getVBucketId());

        MutationStatus ms;
        Item existing = cache.get(command.getKeySpec());

        switch (cmd.getComCode()) {
            case APPEND:
            case APPENDQ:
                ms = cache.append(command.getItem());
                break;
            case PREPEND:
            case PREPENDQ:
                ms = cache.prepend(command.getItem());
                break;
            default:
                return;
        }

        if (ms.getStatus() == ErrorCode.SUCCESS) {
            switch (cmd.getComCode()) {
                case APPEND:
                case PREPEND:
                    client.sendResponse(new BinaryStoreResponse(command, ms, client.getMutinfoWriter(), existing.getCas()));
                default:
                    break;
            }
        } else {
            ErrorCode err = ms.getStatus();
            if (err == ErrorCode.KEY_ENOENT) {
                err = ErrorCode.NOT_STORED;
            }
            client.sendResponse(new BinaryResponse(cmd, err));
        }
    }
}
