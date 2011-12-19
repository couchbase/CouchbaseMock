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
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
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
        String bucketName = exchange.getPrincipal().getName();
        byte[] payload;

        if (path.matches("^/pools/?$")) {
            // GET /pools
            payload = getPoolsJSON().getBytes();
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
            JSONArray buckets = new JSONArray();
            for (Bucket bucket : mock.getBuckets().values()) {
                if (mock.getAuthenticator().getAdminName().equals(bucketName) ||    // Administrator
                        (bucketName.isEmpty() && bucket.getPassword().isEmpty()) || // Public
                        bucket.getName().equals(bucketName)) {                      // Protected
                    buckets.add(JSONObject.fromObject(bucket.getJSON()));
                }
            }
            payload = buckets.toString().getBytes();
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
            do {
                body.write(bucket.getJSON().getBytes());
                body.write("\n\n\n\n".getBytes());
                body.flush();
            } while (mock.waitForUpdate());
        } else {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        }
        body.close();

    }

    protected String getPoolsJSON() {
        Map<String, Object> pools = new HashMap<String, Object>();
        pools.put("name", mock.getPoolName());
        pools.put("uri", "/pools/" + mock.getPoolName());
        pools.put("streamingUri", "/poolsStreaming/" + mock.getPoolName());

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("pools", pools);
        map.put("isAdminCreds", Boolean.TRUE);

        return JSONObject.fromObject(map).toString();
    }
}
