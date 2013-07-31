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

import java.util.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.control.MockCommandDispatcher;
import org.couchbase.mock.memcached.protocol.CommandCode;

/**
 * This returns information about the current mock's supported
 * command set.
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class MockInfoCommandHandler extends MockCommand {
    private final Map<String,Object> result = new HashMap<String, Object>();

    @Override
    public void execute(JsonObject payload, Command command) {
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
    }

    @Override
    public String getResponse() {
        return (new Gson()).toJson(result);
    }

    public MockInfoCommandHandler(CouchbaseMock m) {
        super(m);
    }
}
