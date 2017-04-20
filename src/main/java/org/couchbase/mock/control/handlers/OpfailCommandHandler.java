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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.LinkedList;
import java.util.List;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.memcached.MemcachedServer;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.jetbrains.annotations.NotNull;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OpfailCommandHandler extends MockCommand {
    private static final Logger logger = Logger.getLogger(OpfailCommandHandler.class.getName());

    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock,
            @NotNull Command command, @NotNull JsonObject payload) {

        ErrorCode eCode = ErrorCode.SUCCESS;
        boolean eCodeFound = false;

        JsonElement eServerList = null;
        if (payload.has("servers")) {
            eServerList = payload.get("servers");
        }

        List<Integer> serverList = new LinkedList<Integer>();
        if (eServerList != null) {
            for (JsonElement ix : eServerList.getAsJsonArray()) {
                serverList.add(ix.getAsInt());
            }
        }

        int count = payload.get("count").getAsInt();
        CommandCode cmdCode = null;
        try {
            if (payload.has("name"))
                cmdCode = CommandCode.valueOf( payload.get("name").getAsString() );
        }
        catch (IllegalArgumentException error) {
            return new CommandStatus().fail("Invalid command code " + error);
        }

        int iCode = payload.get("code").getAsInt();
        for (ErrorCode rc : ErrorCode.values()) {
            if (iCode == rc.value()) {
                eCode = rc;
                eCodeFound = true;
                break;
            }
        }
        if (!eCodeFound) {
            return new CommandStatus().fail("Invalid error code");
        }

        logger.finer("Adding fail handler cmdName:" + cmdCode + " count:" + count + " code:" + eCode);
        for (Bucket bucket : mock.getBuckets().values()) {
            MemcachedServer[] servers = bucket.getServers();
            for (int ii = 0; ii < servers.length; ii++) {
                // In this case, size() is easier to read :)
                if (serverList.size() > 0 && !serverList.contains(ii)) {
                    continue;
                }

                servers[ii].updateFailMakerContext(cmdCode, eCode, count);
            }
        }

        return new CommandStatus();
    }
}
