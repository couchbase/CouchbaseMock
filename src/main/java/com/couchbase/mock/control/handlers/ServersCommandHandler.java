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
package com.couchbase.mock.control.handlers;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.control.CommandStatus;
import com.couchbase.mock.control.MockCommand;
import com.couchbase.mock.memcached.MemcachedServer;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

public abstract class ServersCommandHandler extends MockCommand {

    abstract void doServerCommand(MemcachedServer server);

    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        Set<MemcachedServer> servers = new HashSet<MemcachedServer>();
        for (Bucket bucket : mock.getBuckets().values()) {
            for (MemcachedServer server : bucket.getServers()) {
                if (!servers.contains(server)) {
                    doServerCommand(server);
                    servers.add(server);
                }
            }
        }

        // The return from this method is not returned back to the client
        return new CommandStatus().fail();
    }
}
