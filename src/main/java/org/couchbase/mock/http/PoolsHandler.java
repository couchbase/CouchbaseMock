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

import org.apache.http.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.httpio.HttpServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Avseyev
 */
public class PoolsHandler {
    private final CouchbaseMock mock;

    public PoolsHandler(CouchbaseMock mock) {
        this.mock = mock;
    }

    private final HttpRequestHandler poolHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            String payload = StateGrabber.getAllPoolsJSON(mock);
            HandlerUtil.makeJsonResponse(response, payload);
        }
    };

    private final HttpRequestHandler poolsDefaultHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            String payload = StateGrabber.getPoolInfoJSON(mock);
            HandlerUtil.makeJsonResponse(response, payload);
        }
    };

    private final HttpRequestHandler allBucketsHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            List<Bucket> allowedBuckets = new ArrayList<Bucket>(mock.getBuckets().size());
            Authenticator authenticator = mock.getAuthenticator();
            AuthContext authContext = HandlerUtil.getAuth(context, request);

            for (Bucket bucket : mock.getBuckets().values()) {
                if (authenticator.isAuthorizedForBucket(authContext, bucket)) {
                    allowedBuckets.add(bucket);
                }
            }
            String payload = StateGrabber.getAllBucketsJSON(allowedBuckets);
            HandlerUtil.makeJsonResponse(response, payload);
        }
    };

    public void register(HttpServer server) {
        server.register("/pools", poolHandler);
        server.register(String.format("/pools/%s", mock.getPoolName()), poolsDefaultHandler);
        server.register(String.format("/pools/%s/buckets", mock.getPoolName()), allBucketsHandler);
    }
}
