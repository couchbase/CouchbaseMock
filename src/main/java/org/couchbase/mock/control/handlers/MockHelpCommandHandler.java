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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.control.MockCommand;
import org.jetbrains.annotations.NotNull;

/**
 *
 * @author mnunberg
 */
public final class MockHelpCommandHandler extends MockCommand {
    private static final Map<String,Object> helpInfo = new HashMap<String, Object>();
    static {
        List<Object> helpList = new ArrayList<Object>();
        for (Command cmd : Command.values()) {
            helpList.add(cmd.toString().toLowerCase());
        }
        helpInfo.put("Available Commands", helpList);
        helpInfo.put("status", "help");
    }

    public static String getIndentedHelp() {
        StringWriter sw = new StringWriter(0);
        JsonWriter writer = new JsonWriter(sw);
        writer.setIndent(" ");
        GsonBuilder gsB = new GsonBuilder();
        return gsB.setPrettyPrinting().create().toJson(helpInfo);
    }

    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        Map<String,Object> body = new HashMap<String, Object>();

        CommandStatus ret = new CommandStatus();
        body.put("commands", helpInfo);
        ret.setPayload(body);
        return ret;
    }
}
