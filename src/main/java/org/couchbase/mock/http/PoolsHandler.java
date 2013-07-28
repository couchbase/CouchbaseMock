/**
 * Copyright 2011 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.couchbase.mock.http;

import com.sun.net.httpserver.Headers;
import org.couchbase.mock.CouchbaseMock;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.couchbase.mock.Bucket;

/**
 * @author Sergey Avseyev
 */
public class PoolsHandler implements HttpHandler {

    private final CouchbaseMock mock;

    private class ResourceNotFoundException extends Throwable {
    }

    public PoolsHandler(CouchbaseMock mock) {
        this.mock = mock;
    }

    private List<Bucket> getAllowedBuckets(HttpExchange exchange) {
        List<Bucket> bucketList = new LinkedList<Bucket>();
        String httpUser = exchange.getPrincipal().getUsername();
        String adminUser = mock.getAuthenticator().getAdminName();

        for (Bucket bucket : mock.getBuckets().values()) {
            if ( // Public
                    (httpUser.isEmpty() && bucket.getPassword().isEmpty())
                            || // Administrator
                            adminUser.equals(httpUser)
                            || // Protected
                            bucket.getName().equals(httpUser)) {
                bucketList.add(bucket);
            }
        }
        return bucketList;
    }

    private byte[] extractPayload(HttpExchange exchange, String path)
            throws ResourceNotFoundException, IOException {
        byte[] payload = null;

        if (path.matches("^/pools/?$")) {
            // GET /pools
            payload = StateGrabber.getAllPoolsJSON(mock).getBytes();

        } else if (path.matches("^/pools/" + mock.getPoolName() + "$/?")) {
            // GET /pools/:poolName
            payload = StateGrabber.getPoolInfoJSON(mock).getBytes();

        } else if (path.matches("^/pools/" + mock.getPoolName() + "/buckets/?$")) {
            // GET /pools/:poolName/buckets
            payload = StateGrabber.getAllBucketsJSON(
                    getAllowedBuckets(exchange)).getBytes();

        } else if (path.matches("^/pools/" + mock.getPoolName() + "/buckets/[^/]+/?$")) {
            // GET /pools/:poolName/buckets/:bucketName
            String[] tokens = path.split("/");
            Bucket bucket = mock.getBuckets().get(tokens[tokens.length - 1]);
            payload = StateGrabber.getBucketJSON(bucket).getBytes();

        } else if (path.matches("^/pools/" + mock.getPoolName() + "/bucketsStreaming/[^/]+/?$")) {
            // GET /pools/:poolName/bucketsStreaming/:bucketName
            String[] tokens = path.split("/");
            Bucket bucket = mock.getBuckets().get(tokens[tokens.length - 1]);
            if (bucket == null) {
                throw new ResourceNotFoundException();
            }

            exchange.getResponseHeaders().set("Transfer-Encoding", "chunked");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
            BucketsStreamingHandler streamingHandler =
                    new BucketsStreamingHandler(mock.getMonitor(),
                            bucket, exchange.getResponseBody());
            try {
                streamingHandler.startStreaming();
            } catch (InterruptedException ex) {
                Logger.getLogger(PoolsHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {

            throw new ResourceNotFoundException();
        }

        return payload;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        OutputStream body = exchange.getResponseBody();
        Headers responseHeaders = exchange.getResponseHeaders();

        responseHeaders.set("Server", "CouchbaseMock/1.0");

        byte[] payload;

        try {
            payload = extractPayload(exchange, path);
            if (payload != null) {
                responseHeaders.set("Content-Type", "application/json");
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, payload.length);
                body.write(payload);
            } else {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
            }
        } catch (ResourceNotFoundException ex) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        } finally {
            body.close();
        }
    }
}
