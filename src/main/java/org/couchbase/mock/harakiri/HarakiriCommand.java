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
package org.couchbase.mock.harakiri;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.*;
import org.couchbase.mock.CouchbaseMock;
/**
 *
 * @author mnunberg
 */
public abstract class HarakiriCommand {
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

    protected static final Gson gs = new Gson();

    protected JsonObject payload;
    protected Command command;
    protected final CouchbaseMock mock;

    protected void handleJson(JsonObject json) {
    }

    boolean canRespond() {
        return payload != null;
    }

    public String getResponse() {
        return "{\"status\":\"ok\"}";
    }

    public Command getCommand() {
        return command;
    }

    public HarakiriCommand(CouchbaseMock mock) {
        this.mock = mock;
    }

    public abstract void execute();
}
