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
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Implementation of the prepend command
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
class PrependCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        BinaryStoreCommand command = (BinaryStoreCommand) cmd;

        ErrorCode err;
        Item item = command.getItem();

        Item existing = server.getDatastore().get(server, cmd.getVBucketId(), cmd.getKey());
        if (existing == null) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.NOT_STORED));
            return;
        }
        existing.prepend(item);
        err = server.getDatastore().replace(server, cmd.getVBucketId(), existing);
        if (err == ErrorCode.SUCCESS && cmd.getComCode() == CommandCode.PREPENDQ) {
            return;
        }
        client.sendResponse(new BinaryStoreResponse(command, err, existing.getCas()));
    }
}
