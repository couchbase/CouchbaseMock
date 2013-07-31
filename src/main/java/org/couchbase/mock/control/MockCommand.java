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

/**
 * The MockCommand class is the base class for all commands
 * that the client may send to the mock to instruct it to
 * do certain actions.
 *
 * All commands is sent as JSON objects with a certain form:
 *
 * { "command" : "name of the command", "payload" : {  } }
 *
 * The response for the command will be delivered at its most basic
 * level will be a JSON object consisting of the following fields:
 *
 * { "status" : "ok", "payload" : { } }
 *
 * For "non-successful" commands the object may be:
 *
 * { "status" : "fail", "error" : "error description" }
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
        HELP
    }

    protected final CouchbaseMock mock;

    public MockCommand(CouchbaseMock mock) {
        this.mock = mock;
    }

    /**
     * Get the response to send to the client. This <b>must</b> be a
     * valid JSON encoded object according to the protocol. The various
     * implementations may override this method if they want to return
     * anything else than a simple success.
     *
     * @return the string representing success.
     */
    public String getResponse() {
        return "{\"status\":\"ok\"}";
    }

    /**
     * Execute the command
     *
     * @param payload the payload containing arguments to the command
     * @param command the actual command being executed (in case a handler
     *                implements multiple commands
     */
    public abstract void execute(JsonObject payload, Command command);
}
