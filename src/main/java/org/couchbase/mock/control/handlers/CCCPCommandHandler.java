package org.couchbase.mock.control.handlers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.memcached.MemcachedServer;
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
