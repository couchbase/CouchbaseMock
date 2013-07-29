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

import java.security.AccessControlException;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryGetCommand;
import org.couchbase.mock.memcached.protocol.BinaryGetResponse;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class GetCommandExecutor implements CommandExecutor {
    static final int DEFAULT_EXPIRY_TIME = 15;
    static final int MAXIMUM_EXPIRY_TIME = 29;

    @Override
    public void execute(BinaryCommand command, MemcachedServer server, MemcachedConnection client) {
        BinaryGetCommand cmd = (BinaryGetCommand) command;
        VBucketStore cache;
        CommandCode cc = cmd.getComCode();

        if (cc == CommandCode.GET_REPLICA) {
            cache = server.getStorage().getCache(cmd.getVBucketId());
            if (!server.getStorage().getVBucketInfo(cmd.getVBucketId()).hasAccess(server)) {
                throw new AccessControlException("we're not a master or replica");
            }
        } else {
            cache = server.getStorage().getCache(server, cmd.getVBucketId());
        }

        Item item = cache.get(cmd.getKeySpec());

        if (item == null) {
            if (cc != CommandCode.GETKQ && cc != CommandCode.GETQ && cc != CommandCode.GATQ) {
                client.sendResponse(new BinaryGetResponse(cmd, ErrorCode.KEY_ENOENT));
            }
            return;
        }

        if (cc == CommandCode.GETL) {
            if (item.isLocked()) {
                client.sendResponse(new BinaryGetResponse(cmd, ErrorCode.ETMPFAIL));
                return;

            } else {
                int lockExpiryTime = cmd.getExpiration();
                if (lockExpiryTime == 0 || lockExpiryTime > MAXIMUM_EXPIRY_TIME) {
                    lockExpiryTime = DEFAULT_EXPIRY_TIME;
                }
                item.setLockExpiryTime(lockExpiryTime);
                cache.onItemMutated.onAction(cache, item);
            }
        } else if (cc == CommandCode.TOUCH || cc == CommandCode.GAT || cc == CommandCode.GATQ) {
            item.setExpiryTime(cmd.getExpiration());
            cache.onItemMutated.onAction(cache, item);
        }

        if (cc == CommandCode.TOUCH) {
            client.sendResponse(new BinaryGetResponse(cmd, ErrorCode.SUCCESS));
        } else {
            client.sendResponse(new BinaryGetResponse(cmd, item));
        }
    }
}
