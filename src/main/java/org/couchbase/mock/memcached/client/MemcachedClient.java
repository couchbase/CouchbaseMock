package org.couchbase.mock.memcached.client;

import org.couchbase.mock.memcached.MemcachedConnection;
import org.couchbase.mock.memcached.MemcachedServer;

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
