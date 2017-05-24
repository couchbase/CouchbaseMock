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

import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.control.CommandStatus;
import com.couchbase.mock.memcached.MemcachedServer;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

/**
 * Created by mnunberg on 4/12/17.
 */
public class GetCmdLogCommandHandler extends CmdLogCommandHandler {
    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        super.execute(mock, command, payload);
        JsonArray arr = new JsonArray();
        MemcachedServer server = bucket.getServers()[idx];
        for (MemcachedServer.CommandLogEntry ent : server.getLogs()) {
            JsonObject obj = new JsonObject();
            obj.addProperty("opcode", ent.getOpcode());
            obj.addProperty("ms_timestamp", ent.getMsTimestamp());
            arr.add(obj);
        }

        CommandStatus status = new CommandStatus();
        status.setPayload(arr);
        return status;
    }
}
