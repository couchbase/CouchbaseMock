package org.couchbase.mock.http.capi;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.JsonUtils;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.Storage;
import org.couchbase.mock.views.Configuration;
import org.couchbase.mock.views.QueryExecutionException;
import org.couchbase.mock.views.View;

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
        // For now, ignore the configuration
        Iterable<Item> items = bucket.getMasterItems(Storage.StorageType.CACHE);
        String queryString = null;
        Configuration viewParams = null;

        if (request.getRequestLine().getMethod().equals("GET")) {
            URL url = HandlerUtil.getUrl(request);
            queryString = url.getQuery();

        } else if (request.getRequestLine().getMethod().equals("POST")) {
            // Request bodies sent to POST are raw JSON, not URL-encoded

            HttpEntity reqEntity = ((HttpEntityEnclosingRequest)request).getEntity();
            String contentType = reqEntity.getContentType().getValue();
            String rawPayload = EntityUtils.toString(reqEntity);

            if (contentType.equals(ContentType.APPLICATION_JSON.getMimeType())) {
                JsonObject jsonDecoded = JsonUtils.decode(rawPayload, JsonObject.class);
                Map<String,String> confParams = new HashMap<String, String>();

                for (Map.Entry<String,JsonElement> entry : jsonDecoded.entrySet()) {
                    confParams.put(entry.getKey(), entry.getValue().toString());
                }
                viewParams = new Configuration(confParams);

            } else if (contentType.equals(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
                queryString = rawPayload;
            }
        }

        if (queryString != null && !queryString.isEmpty()) {
            try {
                Map<String,String> kvParams = HandlerUtil.getQueryParams(queryString);
                viewParams = new Configuration(kvParams);
            } catch (MalformedURLException ex) {
                String errMessage = ex.getMessage();
                if (errMessage == null) {
                    errMessage = "Internal Error for " + queryString;
                }
                HandlerUtil.makeJsonResponse(response, errMessage);
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                return;
            }
        } else if (viewParams == null) {
            viewParams = new Configuration();
        }

        try {
            String s = view.executeRaw(items, viewParams);
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
