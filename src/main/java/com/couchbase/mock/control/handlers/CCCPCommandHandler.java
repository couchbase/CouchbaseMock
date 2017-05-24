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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.control.MockCommand;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Mark Nunberg
 */
public final class CCCPCommandHandler extends MockCommand {
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

        if (payload.has("servers")) {
            JsonArray arr = payload.get("servers").getAsJsonArray();
            for (int ii = 0; ii < arr.size(); ii++) {
                JsonElement e = arr.get(ii);
                enabledServers.add(e.getAsInt());
            }
        }

        for (Bucket bucket : mock.getBuckets().values()) {
            if (!enabledBuckets.contains(bucket.getName())) {
                continue;
            }
            MemcachedServer[] servers = bucket.getServers();
            for (int ii = 0; ii < servers.length; ii++) {
                //noinspection PointlessBooleanExpression
                if (enabledServers.isEmpty() == false &&
                        enabledServers.contains(ii) == false) {
                    continue;
                }
                servers[ii].setCccpEnabled(enabled);
            }
        }

        return new CommandStatus();
    }
}
