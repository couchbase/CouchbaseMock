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

import java.util.*;

import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.control.CommandStatus;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.google.gson.JsonObject;
import com.couchbase.mock.control.MockCommand;
import com.couchbase.mock.control.MockCommandDispatcher;
import org.jetbrains.annotations.NotNull;

/**
 * This returns information about the current mock's supported
 * command set.
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public final class MockInfoCommandHandler extends MockCommand {
    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        Map<String,Object> result = new HashMap<String, Object>();
        List<String> mcCaps = new ArrayList<String>();
        for (CommandCode cc : CommandCode.values()) {
            mcCaps.add(cc.toString());
        }
        result.put("MEMCACHED", mcCaps);

        List<String> mockCaps = new ArrayList<String>();
        for (String key : MockCommandDispatcher.commandMap.keySet()) {
            mockCaps.add(key);
        }

        result.put("MOCK", mockCaps);

        CommandStatus ret = new CommandStatus();
        ret.setPayload(result);
        return ret;
    }
}
