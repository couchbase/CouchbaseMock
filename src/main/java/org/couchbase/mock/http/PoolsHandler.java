/**
 *     Copyright 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.couchbase.mock.http;

import org.couchbase.mock.CouchbaseMock;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import org.couchbase.mock.Bucket;

/**
 *
 * @author Sergey Avseyev
 */
public class PoolsHandler implements HttpHandler {

    private final CouchbaseMock mock;

    public PoolsHandler(CouchbaseMock mock) {
        this.mock = mock;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        OutputStream body = exchange.getResponseBody();
        byte[] payload;

        if (path.matches("^/pools$")) {
            // GET /pools
            payload = getPoolsJSON();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, payload.length);
            body.write(payload);
        } else if (path.matches("^/pools/" + mock.getPoolName() + "$")) {
            // GET /pools/:poolName
            StringWriter sw = new StringWriter();
            sw.append("{\"buckets\":{\"uri\":\"/pools/" + mock.getPoolName() + "/buckets/default\"}}");
            payload = sw.toString().getBytes();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, payload.length);
            body.write(payload);
        } else if (path.matches("^/pools/" + mock.getPoolName() + "/buckets$")) {
            // GET /pools/:poolName/buckets
            StringWriter sw = new StringWriter();
            sw.append("[");
            for (Bucket bb : mock.getBuckets().values()) {
                sw.append(bb.getJSON());
            }
            sw.append("]");
            payload = sw.toString().getBytes();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, payload.length);
            body.write(payload);
        } else if (path.matches("^/pools/" + mock.getPoolName() + "/buckets/[^/]+$")) {
            // GET /pools/:poolName/buckets/:bucketName
            String[] tokens = path.split("/");
            Bucket bucket = mock.getBuckets().get(tokens[tokens.length - 1]);
            payload = bucket.getJSON().getBytes();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, payload.length);
            body.write(payload);
        } else if (path.matches("^/pools/" + mock.getPoolName() + "/bucketsStreaming/[^/]+$")) {
            // GET /pools/:poolName/bucketsStreaming/:bucketName
            String[] tokens = path.split("/");
            Bucket bucket = mock.getBuckets().get(tokens[tokens.length - 1]);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
            body.write(bucket.getJSON().getBytes());
            body.write("\n\n\n\n".getBytes());
            body.flush();
        } else {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        }
        body.close();
    }

    private byte[] getPoolsJSON() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("{\"pools\":[{\"name\":\"" + mock.getPoolName() + "\",\"uri\":\"/pools/" + mock.getPoolName() + "\","
                + "\"streamingUri\":\"/poolsStreaming/" + mock.getPoolName() + "\"}],\"isAdminCreds\":true,"
                + "\"uuid\":\"f0918647-73a6-4001-15e8-264500000190\",\"implementationVersion\":\"1.7.0\","
                + "\"componentsVersion\":{\"os_mon\":\"2.2.5\",\"mnesia\":\"4.4.17\",\"kernel\":\"2.14.3\","
                + "\"sasl\":\"2.1.9.3\",\"ns_server\":\"1.7.0\",\"stdlib\":\"1.17.3\"}}");
        pw.flush();
        return sw.toString().getBytes();
    }
}
