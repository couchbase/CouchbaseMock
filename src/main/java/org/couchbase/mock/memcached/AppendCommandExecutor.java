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
 * Implementation of the APPEND[Q] command
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
class AppendCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        BinaryStoreCommand command = (BinaryStoreCommand) cmd;

        Item item = command.getItem();

        Item existing = server.getDataStore().get(server, cmd.getVBucketId(), cmd.getKey());
        if (existing == null) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.NOT_STORED));
            return;
        }
        if (!existing.ensureUnlocked(cmd.getCas())) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.ETMPFAIL));
            return;
        }
        existing.append(item);
        ErrorCode err = server.getDataStore().replace(server, cmd.getVBucketId(), existing);
        if (err == ErrorCode.SUCCESS && cmd.getComCode() == CommandCode.APPENDQ) {
            return;
        }
        client.sendResponse(new BinaryStoreResponse(command, err, existing.getCas()));
    }
}
