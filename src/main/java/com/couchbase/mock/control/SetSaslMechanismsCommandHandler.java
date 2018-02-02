/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.mock.control;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.memcached.MemcachedServer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SetSaslMechanismsCommandHandler extends MockCommand {
    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        Set<Integer> enabledServers = new HashSet<Integer>();
        Set<String> enabledBuckets = new HashSet<String>();
        List<String> enabledMechs = new ArrayList<String>();
        for (JsonElement mech : payload.get("mechs").getAsJsonArray()) {
            enabledMechs.add(mech.getAsString());
        }
        loadBuckets(mock, payload, enabledBuckets);
        loadServers(payload, enabledServers);

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
                servers[ii].setSaslMechanisms(enabledMechs);
            }
        }

        return new CommandStatus();
    }
}
