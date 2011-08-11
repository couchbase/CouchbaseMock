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

import org.couchbase.mock.memcached.protocol.ComCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryGetResponse;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class GetCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        Item item = server.getDatastore().get(server, cmd.getVBucketId(), cmd.getKey());
        if (item == null) {
            if (cmd.getComCode() != ComCode.GETKQ && cmd.getComCode() != ComCode.GETQ) {
                client.sendResponse(new BinaryGetResponse(cmd, ErrorCode.KEY_ENOENT));
            }
        } else {
            client.sendResponse(new BinaryGetResponse(cmd, item));
        }
    }
}
