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
import  org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The MockClient utilize a dedicated connection to
 * perform all of the interaction to the mock server.
 */
public class MockClient extends AbstractMockClient {
    private final InetSocketAddress listenAddress;
    private final ServerSocket serverSocket;
    private Socket clientSocket = null;
    private BufferedReader reader = null;
    private BufferedWriter writer = null;
    private int restPort = -1;
    private volatile boolean isShutdown = false;


    public MockClient(@NotNull InetSocketAddress address) throws IOException {
        serverSocket = new ServerSocket(address.getPort(), 10);
        listenAddress = new InetSocketAddress(serverSocket.getInetAddress(),
                serverSocket.getLocalPort());

    }

    @SuppressWarnings("UnusedDeclaration")
    public InetSocketAddress getListeningAddress() {
        return listenAddress;
    }

    @SuppressWarnings("UnusedDeclaration")
    public InetSocketAddress getRestAddress() {
        return new InetSocketAddress(restPort);
    }

    public int getPort() {
        return listenAddress.getPort();
    }

    public int getRestPort() {
        return restPort;
    }

    @Override
    public void negotiate() throws IOException {
        clientSocket = serverSocket.accept();
        reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

        // Get REST port
        StringBuilder sb = new StringBuilder();
        char c;
        while ((c = (char) reader.read()) != '\0') {
            sb.append(c);
        }
        restPort = Integer.parseInt(sb.toString());
    }

    private void sendRequest(MockRequest request) throws IOException {
        String encoded = new Gson().toJson(request.getMap());
        writer.write(encoded, 0, encoded.length());
        writer.write('\n');
        writer.flush();
    }

    private MockResponse readResponse() throws IOException {
        String respLine = reader.readLine();
        return new MockResponse(respLine);
    }

    @Override
    public @NotNull MockResponse request(@NotNull MockRequest request) throws IOException {
        sendRequest(request);
        return readResponse();
    }

    @Override
    public synchronized void shutdown() {
        if (isShutdown) {
            return;
        }

        isShutdown = true;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        if (clientSocket != null) {
            try {
                reader.close();
                writer.close();
                clientSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}