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

import java.util.Map.Entry;

import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryStatResponse;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class StatCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        String key = cmd.getKey();
        if ("uuid".equals(key)) {
            client.sendResponse(new BinaryStatResponse(cmd, "uuid", server.getBucket().getUUID()));
        } else {
            for (MemcachedServer ss : server.getBucket().activeServers()) {
                for (Entry<String, String> stat : ss.getStats().entrySet()) {
                    if (key == null || key.equals(stat.getKey())) {
                        client.sendResponse(new BinaryStatResponse(cmd, stat.getKey(), stat.getValue()));
                    }
                }
            }
        }
        client.sendResponse(new BinaryResponse(cmd, ErrorCode.SUCCESS));
    }
}
