package org.couchbase.mock.control.handlers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.memcached.MemcachedServer;
import org.jetbrains.annotations.NotNull;

/**
 * Needed to get a list of numeric ports for a given
 *
 */
public class GetMCPortsHandler extends MockCommand {
    @Override
    @NotNull
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        String name;
        if (payload.has("bucket")) {
            name = payload.get("bucket").getAsString();
        } else {
            name = "default";
        }



        JsonArray arr = new JsonArray();
        Bucket bucket = mock.getBuckets().get(name);
        if (bucket == null) {
            return new CommandStatus().fail("No such bucket: " + name);
        }
        for (MemcachedServer server : bucket.getServers()) {
            int port = server.getPort();
            arr.add(new JsonPrimitive(port));
        }
        CommandStatus status = new CommandStatus();
        status.setPayload(arr);
        return status;
    }
}
