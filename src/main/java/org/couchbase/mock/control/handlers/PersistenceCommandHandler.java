/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.control.handlers;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.memcached.*;
import org.jetbrains.annotations.NotNull;

/**
 * Handler for various out-of-band key manipulations
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public final class PersistenceCommandHandler extends KeyCommandHandler {

    private String error = null;

    private void executeReal(JsonObject payload, Command command) {
        final String value;
        long cas = 0;
        boolean onMaster;

        JsonElement eOnReplicas;

        Storage masterStore;
        List<Storage> stores = new ArrayList<Storage>();
        onMaster = payload.get("OnMaster").getAsBoolean();

        if (payload.has("CAS")) {
            cas = payload.get("CAS").getAsLong();
        }

        if (payload.has("Value")) {
            value = payload.get("Value").getAsString();
        } else {
            value = "";
        }

        masterStore = vbi.getOwner().getStorage();

        if (onMaster) {
            stores.add(masterStore);
        }

        // Figure out which replicas to affect
        eOnReplicas = payload.get("OnReplicas");
        if (eOnReplicas.isJsonArray()) {
            // An array of indices to use:
            for (JsonElement ix : eOnReplicas.getAsJsonArray()) {
                MemcachedServer mc = vbi.getReplicas().get(ix.getAsInt());
                Storage s = mc.getStorage();
                if (!stores.contains(s)) {
                    stores.add(s);
                }
            }
        } else {
            int maxReplicas = eOnReplicas.getAsInt();
            int replicasSelected = 0;
            for (MemcachedServer server : vbi.getReplicas()) {
                if (replicasSelected == maxReplicas) {
                    break;
                }
                if (!server.isActive()) {
                    continue;
                }
                stores.add(server.getStorage());
                replicasSelected++;
            }
        }

        Item source = masterStore.getCached(keySpec);
        Item newItem;

        if (source == null) {
            assert value != null;
            assert value.getBytes() != null;
            source = new Item(keySpec, 0, 0, value.getBytes(), cas);
        }

        if (cas < 0) {
            cas = (source.getCas() + 1) * 2;
        }

        if (cas != 0) {
            newItem = new Item(
                    source.getKeySpec(),
                    source.getFlags(),
                    source.getExpiryTime(),
                    value.getBytes(),
                    cas);

        } else {
            newItem = new Item(source);
        }

        if (stores.size() == 0) {
            System.err.println("No stores available for key");
        }

        for (Storage curStore : stores) {
            switch (command) {
                case PERSIST:
                case ENDURE:
                    curStore.putPersisted(newItem);
                    if (command == Command.PERSIST) {
                        break;
                    }
                    // ENDURE fallthrough
                case CACHE:
                    curStore.putCached(newItem);
                    break;

                case PURGE:
                case UNPERSIST:
                    curStore.removePersisted(keySpec);
                    if (command == Command.UNPERSIST) {
                        break;
                    }

                case UNCACHE:
                    curStore.removeCached(keySpec);
                    break;

                default:
                    throw new RuntimeException("Unrecognized command");
            }
        }
    }

    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        super.execute(mock, command, payload);
        try {
            executeReal(payload, command);
        } catch (AccessControlException e) {
            error = e.getMessage();
        }

        return getResponse();
    }

    @NotNull
    @Override
    protected CommandStatus getResponse() {
        if (error == null) {
            return super.getResponse();
        }
        return new CommandStatus().fail(error);
    }
}
