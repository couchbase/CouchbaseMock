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

package com.couchbase.mock.http.capi;

import com.couchbase.mock.JsonUtils;
import com.couchbase.mock.memcached.Storage;
import com.couchbase.mock.views.QueryExecutionException;
import com.couchbase.mock.views.View;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.httpio.HandlerUtil;
import com.couchbase.mock.memcached.Item;
import com.couchbase.mock.views.Configuration;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class ViewHandler implements HttpRequestHandler {
    final View view;
    final Bucket bucket;

    public ViewHandler(View view, Bucket bucket) {
        this.view = view;
        this.bucket = bucket;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        Iterable<Item> items = bucket.getMasterItems(Storage.StorageType.CACHE);
        URL url = HandlerUtil.getUrl(request);
        Map<String,String> paramsMap = new HashMap<String, String>();

        if (request.getRequestLine().getMethod().equals("POST")) {
            HttpEntity reqEntity = ((HttpEntityEnclosingRequest)request).getEntity();
            String contentType = reqEntity.getContentType().getValue();
            String rawPayload = EntityUtils.toString(reqEntity);

            if (!contentType.equals(ContentType.APPLICATION_JSON.getMimeType())) {
                HandlerUtil.makeJsonResponse(response, "Content type must be application/json");
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return;
            }

            JsonObject jsonDecoded = JsonUtils.decode(rawPayload, JsonObject.class);
            for (Map.Entry<String,JsonElement> entry : jsonDecoded.entrySet()) {
                paramsMap.put(entry.getKey(), entry.getValue().toString());
            }
        }

        String queryString = url.getQuery();
        if (queryString != null && !queryString.isEmpty()) {
            try {
                Map<String, String> kvParams = HandlerUtil.getQueryParams(queryString);
                paramsMap.putAll(kvParams);
            } catch (MalformedURLException ex) {
                String errMessage = ex.getMessage();
                if (errMessage == null) {
                    errMessage = "Internal Error for " + queryString;
                }
                HandlerUtil.makeJsonResponse(response, errMessage);
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return;
            }
        }

        try {
            String s = view.executeRaw(items, new Configuration(paramsMap));
            HandlerUtil.makeJsonResponse(response, s);
            response.setStatusCode(HttpStatus.SC_OK);
            StringEntity entity = (StringEntity)response.getEntity();
            entity.setChunked(true);

        } catch (QueryExecutionException ex) {
            HandlerUtil.makeJsonResponse(response, ex.getJsonString());
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
        }
    }
}
