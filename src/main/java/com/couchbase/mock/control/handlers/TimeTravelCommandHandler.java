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
import com.couchbase.mock.Info;
import com.couchbase.mock.control.CommandStatus;
import com.couchbase.mock.control.MissingRequiredFieldException;
import com.couchbase.mock.control.MockCommand;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

public final class TimeTravelCommandHandler extends MockCommand {
    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        if (payload.has("Offset")) {
            Info.timeTravel(payload.get("Offset").getAsInt());
        } else {
            throw new MissingRequiredFieldException("Offset");
        }
        return getResponse();
    }
}
