/**
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

import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.jetbrains.annotations.NotNull;

/**
 * The MockCommand class is the base class for all commands
 * that the client may send to the mock to instruct it to
 * do certain actions.
 * <p/>
 * All commands is sent as JSON objects with a certain form:
 * <p/>
 * { "command" : "name of the command", "payload" : {  } }
 * <p/>
 * The response for the command will be delivered at its most basic
 * level will be a JSON object consisting of the following fields:
 * <p/>
 * { "status" : "ok", "payload" : { } }
 * <p/>
 * For "non-successful" commands the object may be:
 * <p/>
 * { "status" : "fail", "error" : "error description" }
 * <p/>
 * To implement a new command you should subclass the MockCommand
 * class and add implement the execute method. You must add the
 * name of the command to the Command enum, and register the
 * class in MockCommandDispatcher.
 *
 * @author mnunberg
 */
public abstract class MockCommand {
    public enum Command {
        FAILOVER,
        RESPAWN,
        HICCUP,
        TRUNCATE,
        MOCKINFO,

        PERSIST,
        CACHE,
        UNPERSIST,
        UNCACHE,
        ENDURE,
        PURGE,

        KEYINFO,
        TIME_TRAVEL,
        HELP,
        OPFAIL,
        SET_CCCP,
        GET_MCPORTS,
        REGEN_VBCOORDS,
        RESET_QUERYSTATE
    }

    /**
     * Get the response to send to the client.
     * @return a CommandStatus object indicating the status
     */
    @NotNull
    protected CommandStatus getResponse() {
        return new CommandStatus();
    }

    /**
     * Execute the command
     *
     * @param mock    the couchbase mock object to operate on
     * @param command the actual command being executed (in case a handler
     *                implements multiple commands
     * @param payload the payload containing arguments to the command
     */
    public abstract
    @NotNull
    CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload);
}
