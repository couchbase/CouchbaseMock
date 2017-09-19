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
import com.couchbase.mock.control.MockCommand;
import com.couchbase.mock.http.query.ErrorState;
import com.couchbase.mock.http.query.QueryServer;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

/**
 * @author Sergey Avseyev
 */
public final class SetQueryErrorStateCommandHandler extends MockCommand {
    @Override
    @NotNull
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        if (payload.has("message") && payload.has("code")) {
            boolean failRegular = true;
            if (payload.has("fail_regular")) {
                failRegular = payload.get("fail_regular").getAsBoolean();
            }
            boolean failPrepared = true;
            if (payload.has("fail_prepared")) {
                failPrepared = payload.get("fail_prepared").getAsBoolean();
            }
            ErrorState state = new ErrorState(payload.get("message").getAsString(),
                    payload.get("code").getAsInt(), failRegular, failPrepared);
            QueryServer.setErrorState(state);
        } else {
            QueryServer.resetErrorState();
        }
        return new CommandStatus();
    }
}
