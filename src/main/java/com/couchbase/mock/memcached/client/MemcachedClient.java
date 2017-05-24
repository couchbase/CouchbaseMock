/*
 * Copyright 2017 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.couchbase.mock.memcached.client;

import com.couchbase.mock.memcached.MemcachedServer;
import com.couchbase.mock.memcached.MemcachedConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by mnunberg on 1/15/14.
 */
public class MemcachedClient {
    final Socket socket;
    final InputStream input;
    final OutputStream output;

    public MemcachedClient(Socket socket) throws IOException {
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    public ClientResponse readResponse() throws IOException {
        return ClientResponse.read(input);
    }

    public ClientResponse sendRequest(byte[] req) throws IOException {
        if (req.length < 24) {
            throw new IllegalArgumentException("Header too small..");
        }
        output.write(req);
        output.flush();
        return readResponse();
    }

    public ClientResponse sendRequest(CommandBuilder builder) throws IOException {
        return sendRequest(builder.build());
    }

    public void close() throws IOException  {
        socket.close();
        input.close();
        output.close();
    }

    public MemcachedConnection getConnection(MemcachedServer server) throws IOException {
        return server.findConnection(socket.getLocalSocketAddress());
    }
}
