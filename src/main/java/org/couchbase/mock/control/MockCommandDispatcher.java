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
package org.couchbase.mock.control;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.handlers.*;
import org.jetbrains.annotations.NotNull;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Mark Nunberg
 */
public class MockCommandDispatcher {
    public static final Map<String, Class> commandMap = new HashMap<String, Class>();
    private static final Map<MockCommand.Command, Class> classMap
            = new EnumMap<MockCommand.Command, Class>(MockCommand.Command.class);
    private static final Gson gs = new Gson();

    private static void registerClass(MockCommand.Command cmd, Class cls) {
        if (!MockCommand.class.isAssignableFrom(cls)) {
            throw new RuntimeException("Can process only HarakiriMonitor objects");
        }

        String commandName = cmd.toString().toUpperCase();

        commandMap.put(commandName, cls);
        classMap.put(cmd, cls);
    }

    static {
        registerClass(MockCommand.Command.HICCUP, HiccupCommandHandler.class);
        registerClass(MockCommand.Command.FAILOVER, FailoverCommandHandler.class);
        registerClass(MockCommand.Command.TRUNCATE, TruncateCommandHandler.class);
        registerClass(MockCommand.Command.RESPAWN, RespawnCommandHandler.class);
        registerClass(MockCommand.Command.MOCKINFO, MockInfoCommandHandler.class);
        registerClass(MockCommand.Command.CACHE, PersistenceCommandHandler.class);
        registerClass(MockCommand.Command.UNCACHE, PersistenceCommandHandler.class);
        registerClass(MockCommand.Command.PERSIST, PersistenceCommandHandler.class);
        registerClass(MockCommand.Command.UNPERSIST, PersistenceCommandHandler.class);
        registerClass(MockCommand.Command.ENDURE, PersistenceCommandHandler.class);
        registerClass(MockCommand.Command.PURGE, PersistenceCommandHandler.class);
        registerClass(MockCommand.Command.KEYINFO, KeyInfoCommandHandler.class);
        registerClass(MockCommand.Command.TIME_TRAVEL, TimeTravelCommandHandler.class);
        registerClass(MockCommand.Command.HELP, MockHelpCommandHandler.class);
        registerClass(MockCommand.Command.OPFAIL, OpfailCommandHandler.class);
        registerClass(MockCommand.Command.SET_CCCP, CCCPCommandHandler.class);
        registerClass(MockCommand.Command.GET_MCPORTS, GetMCPortsHandler.class);
        registerClass(MockCommand.Command.REGEN_VBCOORDS, RegenCoordsHandler.class);
        registerClass(MockCommand.Command.RESET_QUERYSTATE, ResetQueryStateHandler.class);
    }


    // Instance members
    private final CouchbaseMock mock;

    public @NotNull CommandStatus dispatch(String command, JsonObject payload) {
        MockCommand obj;

        command = command.replaceAll(" ", "_").toUpperCase();

        if (!commandMap.containsKey(command)) {
            throw new CommandNotFoundException("Unknown command: " + command);
        }

        MockCommand.Command cmd;
        Class cls;

        try {
            cmd = MockCommand.Command.valueOf(command.toUpperCase());

        } catch (IllegalArgumentException e) {
            throw new CommandNotFoundException("No such command: " + command, e);
        }

        cls = classMap.get(cmd);
        if (cls == null) {
            throw new RuntimeException("Can't find class for " + cmd);
        }

        try {
            obj = (MockCommand) cls.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return obj.execute(mock, cmd, payload);
    }

    public MockCommandDispatcher(CouchbaseMock mock) {
        this.mock = mock;
    }

    public CouchbaseMock getMock() {
        return mock;
    }

    /**
     * Process the input sent from the client utilizing the mock server
     * and return the response.
     *
     * @param input the JSON encoded command from the user
     * @return a string to send to the client
     */
    public String processInput(String input) {

        JsonObject object;
        try {
            object = gs.fromJson(input, JsonObject.class);
        } catch (Throwable t) {
            return "{ \"status\" : \"fail\", \"error\" : \"Failed to parse input\" }";
        }

        String command = object.get("command").getAsString();
        JsonObject payload;
        if (!object.has("payload")) {
            payload = new JsonObject();
        } else {
            payload = object.get("payload").getAsJsonObject();
        }

        CommandStatus status;

        try {
            status = dispatch(command, payload);
        } catch (CommandNotFoundException ex) {
            status = new CommandStatus();
            status.fail("No such command");
        } catch (Throwable t) {
            status = new CommandStatus();
            status.fail(t);
        }

        return status.toString();
    }
}
