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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Sergey Avseyev
 */
public final class EnhancedErrorsCommandHandler extends MockCommand {
    @Override
    @NotNull
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        Set<String> enabledBuckets = new HashSet<String>();
        Set<Integer> enabledServers = new HashSet<Integer>();
        boolean enabled = payload.get("enabled").getAsBoolean();

        if (payload.has("bucket")) {
            enabledBuckets.add(payload.get("bucket").getAsString());
        } else {
            enabledBuckets.addAll(mock.getBuckets().keySet());
        }

        boolean enabledForAllServers = false;
        if (payload.has("servers")) {
            JsonArray arr = payload.get("servers").getAsJsonArray();
            for (int ii = 0; ii < arr.size(); ii++) {
                enabledServers.add(arr.get(ii).getAsInt());
            }
        } else {
            enabledForAllServers = true;
        }

        for (Bucket bucket : mock.getBuckets().values()) {
            if (!enabledBuckets.contains(bucket.getName())) {
                continue;
            }
            MemcachedServer[] servers = bucket.getServers();
            for (int ii = 0; ii < servers.length; ii++) {
                if (enabledServers.contains(ii) || enabledForAllServers) {
                    servers[ii].setEnhancedErrorsEnabled(enabled);
                }
            }
        }

        return new CommandStatus();
    }
}
