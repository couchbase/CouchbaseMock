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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.*;

public class HarakiriMonitor extends Observable implements Runnable {

    private final boolean terminate;
    private final CouchbaseMock mock;
    private BufferedReader input;
    private OutputStream output;
    private Socket sock;
    private Thread thread;
    private final Map<String, MockControlCommandHandler> commandHandlers;

    public HarakiriMonitor(String host, int port, boolean terminate, CouchbaseMock mock) throws IOException {
        this.mock = mock;
        this.terminate = terminate;
        sock = new Socket(host, port);
        input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        output = sock.getOutputStream();

        commandHandlers = new HashMap<String, MockControlCommandHandler>();
        commandHandlers.put("failover", new FailoverCommandHandler());
        commandHandlers.put("respawn", new RespawnCommandHandler());
        commandHandlers.put("hiccup", new HiccupCommandHandler());
        commandHandlers.put("truncate", new TruncateCommandHandler());

    }

    public void start() {
        thread = new Thread(this, "HarakiriMonitor");
        thread.start();
    }

    public void stop() {
        thread.interrupt();
    }

    private void dispatchMockCommand(String packet) {
        List<String> tokens = new ArrayList<String>();
        tokens.addAll(Arrays.asList(packet.split(",")));
        String command = tokens.remove(0);

        MockControlCommandHandler handler = commandHandlers.get(command);

        if (handler == null) {
            System.err.printf("Unknown command '%s'\n", command);
            return;
        }

        try {
            handler.execute(mock, tokens);
        } catch (NumberFormatException ex) {
            System.err.printf("Got exception: %s\n", ex.toString());
            return;
        }
        setChanged();
        notifyObservers();
    }

    @Override
    public void run() {
        boolean closed = false;
        String packet;
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
                } else if (mock != null) {
                    dispatchMockCommand(packet);
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
