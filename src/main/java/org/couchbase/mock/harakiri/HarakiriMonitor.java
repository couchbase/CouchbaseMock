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
import java.util.Observable;
import java.util.concurrent.Callable;

import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.MockCommandDispatcher;

/**
 * The HarakiriMonitor started off as a class that was designed to
 * make sure that the mock server would be detecting if the process
 * utilizing it "died" so that it would perform a harakiri.
 *
 * Later on we wanted to send commands to the Mock server over this
 * connection, which sort of made the name misleading...
 */
public class HarakiriMonitor extends Observable implements Runnable {
    private final MockCommandDispatcher dispatcher;
    private Callable onTerminate = null;

    private BufferedReader input = null;
    private OutputStream output;
    private Thread thread;

    public HarakiriMonitor(MockCommandDispatcher dispatcher) throws IOException {
        this.dispatcher = dispatcher;
    }

    public void connect(String host, int port) throws IOException {
        if (input != null) {
            throw new IOException("Already have socket");
        }

        Socket sock = new Socket(host, port);
        input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        output = sock.getOutputStream();
    }

    public void start() {
        if (input == null) {
            throw new IllegalStateException("Not bound yet");
        }

        thread = new Thread(this, "HarakiriMonitor");
        thread.start();
    }

    public void stop() {
        thread.interrupt();
    }

    public void setTemrinateAction(Callable action) {
        this.onTerminate = action;
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
                    String response = dispatcher.processInput(packet);
                    setChanged();
                    notifyObservers();

                    if (response != null) {
                        output.write((response + "\n").getBytes());
                        output.flush();
                    }
                }
            } catch (IOException e) {
                // not exactly true, but who cares..
                closed = true;
            }
        }

        if (onTerminate != null) {
            try {
                onTerminate.call();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
