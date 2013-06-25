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
import java.lang.reflect.InvocationTargetException;
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
        TRUNCATE
    }

    private static final Map<String,Command> commandMap;
    static {
        commandMap = new HashMap<String, Command>();

        commandMap.put("failover", Command.FAILOVER);
        commandMap.put("respawn", Command.RESPAWN);
        commandMap.put("hiccup", Command.HICCUP);
        commandMap.put("truncate", Command.TRUNCATE);
    }

    private static final Map<Command, Class> classMap = new EnumMap<Command, Class>(Command.class);

    protected static void registerClass(Command cmd, Class cls) {
        if (!HarakiriCommand.class.isAssignableFrom(cls)) {
            throw new RuntimeException("Can process only HarakiriMonitor objects");
        }

        classMap.put(cmd, cls);
    }

    private static final Gson gs = new Gson();

    protected JsonObject payload;
    protected Command command;
    protected CouchbaseMock mock;

    protected void handleJson(JsonObject json) {
        payload = json.get("payload").getAsJsonObject();
    }

    protected void handlePlain(List<String> tokens) {
        throw new RuntimeException("This command does not support raw strings");
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

    public static HarakiriCommand getCommand(CouchbaseMock mock, String s)
    {
        JsonObject payload = null;
        ArrayList<String> compat = null;
        String cmdstr;
        HarakiriCommand obj;

        if (s.startsWith("{")) {
            // JSON
            JsonObject o = gs.fromJson(s, JsonObject.class);
            cmdstr = o.get("command").getAsString();
            payload = o.get("payload").getAsJsonObject();
        } else {
            compat = new ArrayList<String>();
            compat.addAll(Arrays.asList(s.split(",")));
            cmdstr = compat.get(0);
            compat.remove(0);
        }

        if (!commandMap.containsKey(cmdstr)) {
            throw new RuntimeException("Unknown command: " + cmdstr);
        }
        Command cmd = commandMap.get(cmdstr);
        Class cls = classMap.get(cmd);
        if (cls == null) {
            throw new RuntimeException("Can't find class for " + cmd);
        }

        try {
            obj = (HarakiriCommand)cls.getConstructor
                    (CouchbaseMock.class).newInstance(mock);

        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        if (payload != null) {
            obj.handleJson(payload);
            obj.payload = payload;
        } else {
            obj.handlePlain(compat);
        }

        return obj;
    }

    public HarakiriCommand(CouchbaseMock mock) {
        this.mock = mock;
    }

    public abstract void execute();
}
