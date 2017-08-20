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
import com.couchbase.mock.memcached.errormap.ErrorMap;
import com.couchbase.mock.memcached.errormap.RetrySpec;
import com.couchbase.mock.memcached.errormap.Verifier;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
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
        int errcode = payload.get("errcode").getAsInt();
        long fuzzms = 0;
        if (payload.has("fuzz_ms")) {
            fuzzms = payload.get("fuzz_ms").getAsLong();
        }

        // Get the logs from the server
        List<MemcachedServer.CommandLogEntry> entries = new ArrayList<MemcachedServer.CommandLogEntry>(bucket.getServers()[idx].getLogs());
        bucket.getServers()[idx].stopLog();

        // Get the spec for the error code
        RetrySpec spec = ErrorMap.DEFAULT_ERRMAP.getErrorEntry(errcode).getRetrySpec();

        try {
            Verifier.verifyThrow(entries, spec, opcode, fuzzms);
            return new CommandStatus();
        } catch (Verifier.VerificationException ex) {
            return new CommandStatus().fail(ex);
        }
    }
}
