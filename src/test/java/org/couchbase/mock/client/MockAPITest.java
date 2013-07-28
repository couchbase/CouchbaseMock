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
package org.couchbase.mock.client;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the "extended" Mock API.
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class MockAPITest extends ClientBaseTest {

    protected class Command {
        Map<String,Object> commandMap = new HashMap<String, Object>();
        Map<String,Object> payload = new HashMap<String, Object>();

        void setName(String name) {
            commandMap.put("command", name);
        }

        String getName() {
            return (String)commandMap.get("command");
        }

        void set(String param, Object value) {
            payload.put(param, value);
        }

        void clear() {
            commandMap.clear();
            payload.clear();
            commandMap.put("payload", payload);
        }

        public Command() {
            commandMap.put("payload", payload);
        }
    }

    private interface CommandSender {
        JsonObject transact(Command cmd) throws Exception;
    }


    private static Gson gs = new Gson();

    protected JsonObject jsonLineTxn(Command command) throws Exception {
        harakiriOutput.write(gs.toJson(command.commandMap).getBytes());
        harakiriOutput.write('\n');

        String line = harakiriInput.readLine();
        assertNotNull(line);

        JsonObject ret = gs.fromJson(line, JsonObject.class);
        assertNotNull(ret);

        return ret;
    }

    protected JsonObject jsonHttpTxn(Command command) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("http://localhost:")
                .append(mock.getHttpPort())
                .append("/mock/")
                .append(command.getName())
                .append("?");

        for (Map.Entry<String,Object> kv : command.payload.entrySet()) {
            String jStr = gs.toJson(kv.getValue());
            jStr = URLEncoder.encode(jStr, "UTF-8");
            sb.append(kv.getKey()).append('=').append(jStr).append('&');
        }

        int index = sb.lastIndexOf("&");
        if (index > 0) {
            sb.deleteCharAt(index);
        }

        URL url = new URL(sb.toString());
        HttpURLConnection uc = (HttpURLConnection)url.openConnection();
        uc.connect();

        sb = new StringBuilder();
        byte[] buf = new byte[4096];
        int nr;
        while ( (nr = uc.getInputStream().read(buf)) != -1) {
            sb.append(new String(buf, 0, nr));
        }


        return gs.fromJson(sb.toString(), JsonElement.class).getAsJsonObject();
    }

    private void doTestSimpleEndure(CommandSender sender) throws Exception {
        Command cmd = new Command();
        cmd.setName("help");
        JsonObject response = sender.transact(cmd);
        assertTrue(response.has("status"));

        // Try something more complicated
        cmd.clear();
        cmd.setName("endure");
        cmd.set("Key", "helloworld");
        cmd.set("Value", "newvalue");
        cmd.set("OnMaster", true);
        cmd.set("OnReplicas", 2);

        response = sender.transact(cmd);
        assertTrue(response.has("status"));
        Object result = client.get("helloworld");
        assertNotNull(result);
        assertEquals("newvalue", (String)result);
    }

    public void testLineProtocol() throws Exception {
        CommandSender sender = new CommandSender() {

            @Override
            public JsonObject transact(Command cmd) throws Exception {
                return jsonLineTxn(cmd);
            }
        };

        doTestSimpleEndure(sender);
    }


    public void testHttpProtocol() throws Exception {
        CommandSender sender = new CommandSender() {

            @Override
            public JsonObject transact(Command cmd) throws Exception {
                return jsonHttpTxn(cmd);
            }
        };

        doTestSimpleEndure(sender);
    }

}
