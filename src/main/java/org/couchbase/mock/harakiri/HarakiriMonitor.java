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
package org.couchbase.mock.harakiri;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.harakiri.HarakiriDispatcher.PayloadFormat;

public class HarakiriMonitor extends Observable implements Runnable {

    private final boolean terminate;
    private BufferedReader input;
    private OutputStream output;
    private Thread thread;
    private final HarakiriDispatcher dispatcher;
    private static final Gson gs = new Gson();

    public HarakiriMonitor(String host, int port, boolean terminate, HarakiriDispatcher dispatcher) throws IOException {
        this.dispatcher = dispatcher;
        this.terminate = terminate;
        Socket sock = new Socket(host, port);
        input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        output = sock.getOutputStream();
    }

    public void start() {
        thread = new Thread(this, "HarakiriMonitor");
        thread.start();
    }

    public void stop() {
        thread.interrupt();
    }

    private HarakiriCommand dispatchMockCommand(String s) {
        Object payloadObj;
        JsonObject payload;
        ArrayList<String> compat;
        String cmdstr;
        HarakiriCommand ret;
        PayloadFormat fmt;

        if (s.startsWith("{")) {
            // JSON
            JsonObject o = gs.fromJson(s, JsonObject.class);
            cmdstr = o.get("command").getAsString();
            if (!o.has("payload")) {
                payload = new JsonObject();
            } else {
                payload = o.get("payload").getAsJsonObject();
            }
            fmt = PayloadFormat.JSON;
            payloadObj = payload;

        } else {
            compat = new ArrayList<String>();
            compat.addAll(Arrays.asList(s.split(",")));
            cmdstr = compat.get(0);
            compat.remove(0);
            fmt = PayloadFormat.PLAIN;
            payloadObj = compat;
        }

        ret = dispatcher.getCommand(fmt, cmdstr, payloadObj);

        setChanged();
        notifyObservers();
        return ret;
    }

    @Override
    public void run() {
        boolean closed = false;
        String packet;
        CouchbaseMock mock = dispatcher.getMock();
        try {
            mock.waitForStartup();
            String http = "" + mock.getHttpPort() + '\0';
            output.write(http.getBytes());
            output.flush();
        } catch (InterruptedException ex) {
            closed = true;
        } catch (IOException ex) {
            closed = true;
        }
        while (!closed) {
            try {
                packet = input.readLine();
                if (packet == null) {
                    closed = true;
                } else {
                    HarakiriCommand cmd = dispatchMockCommand(packet);
                    if (cmd.canRespond()) {
                        output.write((cmd.getResponse() + "\n").getBytes());
                        output.flush();
                    }
                }
            } catch (IOException e) {
                // not exactly true, but who cares..
                closed = true;
            }
        }

        if (terminate) {
            System.exit(1);
        }
    }
}
