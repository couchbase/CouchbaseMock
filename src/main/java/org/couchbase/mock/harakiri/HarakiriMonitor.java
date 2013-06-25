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

    static {
        HarakiriCommand.registerClass(HarakiriCommand.Command.HICCUP,
                HiccupCommandHandler.class);

        HarakiriCommand.registerClass(HarakiriCommand.Command.FAILOVER,
                FailoverCommandHandler.class);

        HarakiriCommand.registerClass(HarakiriCommand.Command.TRUNCATE,
                TruncateCommandHandler.class);

        HarakiriCommand.registerClass(HarakiriCommand.Command.RESPAWN,
                RespawnCommandHandler.class);
    }

    public HarakiriMonitor(String host, int port, boolean terminate, CouchbaseMock mock) throws IOException {
        this.mock = mock;
        this.terminate = terminate;
        sock = new Socket(host, port);
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

    private HarakiriCommand dispatchMockCommand(String packet) {
        HarakiriCommand cmd = HarakiriCommand.getCommand(mock, packet);
        cmd.execute();
        setChanged();
        notifyObservers();
        return cmd;
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
