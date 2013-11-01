/**
 *     Copyright 2012 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.couchbase.mock.control.handlers;

import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;

import org.couchbase.mock.memcached.MemcachedServer;
import org.jetbrains.annotations.NotNull;

public final class TruncateCommandHandler extends ServersCommandHandler {

    private int truncateLimit;

    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        truncateLimit = payload.get("limit").getAsInt();
        super.execute(mock, command, payload);
        return getResponse();
    }

    @Override
    void doServerCommand(MemcachedServer server) {
        server.setTruncateLimit(truncateLimit);
    }
}
