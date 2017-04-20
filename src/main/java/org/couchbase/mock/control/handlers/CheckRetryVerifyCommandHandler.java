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

package org.couchbase.mock.control.handlers;

import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.memcached.MemcachedServer.CommandLogEntry;
import org.couchbase.mock.memcached.errormap.ErrorMap;
import org.couchbase.mock.memcached.errormap.RetrySpec;
import org.couchbase.mock.memcached.errormap.Verifier;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Created by mnunberg on 4/20/17.
 */
public class CheckRetryVerifyCommandHandler extends  BucketCommandHandler {
    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        super.execute(mock, command, payload);
        int opcode = payload.get("opcode").getAsInt();
        // Get the logs from the server
        List<CommandLogEntry> entries = bucket.getServers()[idx].getLogs();
        bucket.getServers()[idx].stopLog();

        // Get the spec for the error code
        RetrySpec spec = ErrorMap.DEFAULT_ERRMAP.getErrorEntry(opcode).getRetrySpec();
        if (!Verifier.verify(entries, spec, opcode)) {
            return new CommandStatus().fail();
        } else {
            return new CommandStatus();
        }
    }
}
