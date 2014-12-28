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

import com.google.gson.JsonElement;
import com.sun.net.httpserver.HttpHandler;
import org.apache.http.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.couchbase.mock.*;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.httpio.HttpServer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;

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

    private final HttpRequestHandler sampleBucketsHandler = new HttpRequestHandler() {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            AuthContext authContext = HandlerUtil.getAuth(context, request);
            if (!mock.getAuthenticator().isAdministrator(authContext)) {
                response.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
                return;
            }

            if (!request.getRequestLine().getMethod().equals("POST")) {
                HandlerUtil.makeResponse(response, "Not Found", HttpStatus.SC_NOT_FOUND);
                return;
            }

            // Parse the JSON
            if (! (request instanceof HttpEntityEnclosingRequest)) {
                HandlerUtil.make400Response(response, "Must have body");
                return;
            }

            String rawBody = EntityUtils.toString(((HttpEntityEnclosingRequest) request).getEntity());
            JsonElement elem = JsonUtils.GSON.fromJson(rawBody, JsonElement.class);
            if (!elem.isJsonArray()) {
                HandlerUtil.make400Response(response, "Request must be JSON array");
                return;
            }
            for (JsonElement mem : elem.getAsJsonArray()) {
                if (!mem.isJsonPrimitive()) {
                    HandlerUtil.make400Response(response, "Element must be string");
                    return;
                }

                String s = mem.getAsString();
                if (!s.equals("beer-sample")) {
                    HandlerUtil.make400Response(response, String.format("[\"Sample %s is not a valid sample.\"]", s));
                    return;
                }

                // Load the bucket!
                try {
                    BucketConfiguration newConfig = new BucketConfiguration(mock.getDefaultConfig());
                    newConfig.name = "beer-sample";
                    try {
                        mock.createBucket(newConfig);
                    } catch (BucketAlreadyExistsException ex) {
                        HandlerUtil.make400Response(response, "[\"Sample beer-sample is already loaded.\"]");
                        return;
                    }
                    DocumentLoader.loadBeerSample(mock);
                } catch (IOException ex) {
                    HandlerUtil.makeResponse(response, ex.toString(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
                }
            }
        }
    };

    class CreateBucketBadParamsException extends Exception {
        CreateBucketBadParamsException(String message) {
            super(message);
        }
    }

    private final HttpRequestHandler allBucketsHandler = new HttpRequestHandler() {
        private void handleListBuckets(HttpRequest request, HttpResponse response, AuthContext authContext) throws HttpException, IOException{
            List<Bucket> allowedBuckets = new ArrayList<Bucket>(mock.getBuckets().size());
            Authenticator authenticator = mock.getAuthenticator();

            for (Bucket bucket : mock.getBuckets().values()) {
                if (authenticator.isAuthorizedForBucket(authContext, bucket)) {
                    allowedBuckets.add(bucket);
                }
            }
            String payload = StateGrabber.getAllBucketsJSON(allowedBuckets);
            HandlerUtil.makeJsonResponse(response, payload);
        }

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            String methodName = request.getRequestLine().getMethod();
            AuthContext authContext = HandlerUtil.getAuth(context, request);

            if (methodName.equals("GET")) {
                handleListBuckets(request, response, authContext);
            } else if (methodName.equals("POST")) {
                if (!mock.getAuthenticator().isAdministrator(authContext)) {
                    response.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
                } else {
                    try {
                        // Create new bucket
                        handleCreateBucket(request, response, context);
                    } catch (CreateBucketBadParamsException ex) {
                        // The docs say 204, however when trying this out on my own, i get 500s all the time. ohwell
                        HandlerUtil.makeResponse(response, ex.getMessage(), HttpStatus.SC_BAD_REQUEST);
                    }
                }
            }
        }
    };

    public void handleCreateBucket(HttpRequest request, HttpResponse response, HttpContext context)
        throws HttpException, IOException, CreateBucketBadParamsException {
        HttpEntityEnclosingRequest entRequest;
        if (! (request instanceof HttpEntityEnclosingRequest)) {
            throw new CreateBucketBadParamsException("Must provide bucket parameters");
        }

        BucketConfiguration config = new BucketConfiguration(mock.getDefaultConfig());

        entRequest = (HttpEntityEnclosingRequest) request;
        Map<String,String> params = HandlerUtil.getQueryParams(EntityUtils.toString(entRequest.getEntity()));

        String name = params.get("name");
        String authType = params.get("authType");

        if (name == null || name.isEmpty()) {
            throw new CreateBucketBadParamsException("Must provide bucket name");
        }

        config.name = name;

        if (authType == null || authType.isEmpty()) {
            throw new CreateBucketBadParamsException("authType must be specified");
        } else if (authType.equals("none")) {
            HandlerUtil.makeResponse(response, "Non-SASL auth not supported", HttpStatus.SC_INTERNAL_SERVER_ERROR);
            return;
        } else if (authType.equals("sasl")) {
            // OK. but we handle this later
            config.password = params.get("saslPassword");
            if (config.password == null) {
                config.password = "";
            }
        } else {
            throw new CreateBucketBadParamsException(
                    "authType must be 'sasl' or 'none' (note 'none' is not supported, but is valid)");
        }

        try {
            String sReplicas = params.get("replicaNumber");
            if (sReplicas != null) {
                config.numReplicas = Integer.parseInt(sReplicas);
                if (config.numReplicas > config.numNodes-1) {
                    throw new CreateBucketBadParamsException("Not enough nodes for replicas");
                }
            }
            String sQuota = params.get("ramQuotaMB");
            if (sQuota == null) {
                throw new CreateBucketBadParamsException("ramQuotaMB missing (but we ignore it)");
            }
            int iQuota = Integer.parseInt(sQuota);
            if (iQuota < 100) {
                throw new CreateBucketBadParamsException("Ram quota must be greater than 100");
            }
        } catch (NumberFormatException ex) {
            throw new CreateBucketBadParamsException("Bad numeric value");
        }

        try {
            mock.createBucket(config);
            response.setStatusCode(HttpStatus.SC_ACCEPTED);
        } catch (BucketAlreadyExistsException ex) {
            HandlerUtil.makeResponse(response, ex.getMessage(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public void handleDeleteBucket(HttpRequest request, HttpResponse response, HttpContext context, Bucket bucket)
            throws HttpException, IOException {

        AuthContext authContext = HandlerUtil.getAuth(context, request);
        Authenticator authenticator = mock.getAuthenticator();
        if (!authenticator.isAdministrator(authContext)) {
            response.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
            return;
        }

        try {
            mock.removeBucket(bucket.getName());
        } catch (FileNotFoundException ex) {
            response.setStatusCode(HttpStatus.SC_NOT_FOUND);
        }
    }

    public void register(HttpServer server) {
        server.register("/pools", poolHandler);
        server.register(String.format("/pools/%s", mock.getPoolName()), poolsDefaultHandler);
        server.register(String.format("/pools/%s/buckets", mock.getPoolName()), allBucketsHandler);
        server.register("/sampleBuckets/install", sampleBucketsHandler);
    }
}
