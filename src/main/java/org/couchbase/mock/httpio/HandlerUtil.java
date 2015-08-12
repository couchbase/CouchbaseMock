package org.couchbase.mock.httpio;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.couchbase.mock.http.AuthContext;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Various utilities for use by handlers and {@link org.couchbase.mock.httpio.HttpServer}
 */
public class HandlerUtil {
    private HandlerUtil() {}

    /**
     * Get the raw httpcomponents ServerConnection object
     * @param cx The context
     * @return The connection for this request
     */
    public static HttpServerConnection getConnection(HttpContext cx) {
        return (HttpServerConnection) cx.getAttribute(HttpCoreContext.HTTP_CONNECTION);
    }

    /**
     * Get the underlying raw Socket for this request
     * @param cx The context
     * @return The raw socket
     */
    public static Socket getSocket(HttpContext cx) {
        return (Socket) cx.getAttribute(HttpServer.CX_SOCKET);
    }

    /**
     * @param request The request
     * @return a URL object for the request
     * @throws MalformedURLException
     */
    public static URL getUrl(HttpRequest request) throws MalformedURLException{
        String uriStr = request.getRequestLine().getUri();
        try {
            return URI.create(uriStr).toURL();
        } catch (IllegalArgumentException ex) {
            return URI.create("http://dummy/" + uriStr).toURL();
        }
    }

    /**
     * Parses a url-encoded query string and
     * @param url The URL to decode
     * @return a map of keys and JSON Values
     * @throws MalformedURLException If one of the values is not JSON
     */
    public static JsonObject getJsonQuery(URL url) throws MalformedURLException {
        String query = url.getQuery();
        JsonObject payload = new JsonObject();
        JsonParser parser = new JsonParser();

        if (query == null) {
            return null;
        }

        for (String kv : query.split("&")) {
            String[] parts = kv.split("=");

            if (parts.length != 2) {
                throw new MalformedURLException();
            }

            String optName = parts[0];
            JsonElement optVal;
            try {
                optVal = parser.parse(URLDecoder.decode(parts[1], "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new MalformedURLException();
            }

            payload.add(optName, optVal);
        }
        return payload;
    }

    /**
     * Get traditional query parameters as a Java map
     * @param s The query string
     * @return A Java map with key-value pairs derived from the query string
     * @throws MalformedURLException If the query string is malformed.
     */
    public static Map<String,String> getQueryParams(String s) throws MalformedURLException {
        Map<String,String> params = new HashMap<String, String>();

        for (String kv : s.split("&")) {
            String[] parts = kv.split("=");
            if (parts.length != 2) {
                throw new MalformedURLException();
            }
            try {
                String k = URLDecoder.decode(parts[0], "UTF-8");
                String v = URLDecoder.decode(parts[1], "UTF-8");
                params.put(k, v);
            } catch (UnsupportedEncodingException ex) {
                throw new MalformedURLException(ex.getMessage());
            }
        }
        return params;
    }

    /**
     * Sets a string as the response
     * @param response The response object
     * @param s The response body
     */
    public static void makeStringResponse(HttpResponse response, String s) {
        StringEntity entity = new StringEntity(s, ContentType.TEXT_PLAIN);
        entity.setContentEncoding("utf-8");
        response.setEntity(entity);
    }

    /**
     * Sets the response body and status
     * @param response The response object
     * @param msg The response body
     * @param status The response status
     */
    public static void makeResponse(HttpResponse response, String msg, int status) {
        response.setStatusCode(status);
        makeStringResponse(response, msg);
    }

    /**
     * Sets a 404 not found response with a message
     * @param response The response object
     * @param msg The message
     */
    public static void make400Response(HttpResponse response, String msg) {
        makeResponse(response, msg, HttpStatus.SC_BAD_REQUEST);
    }

    /**
     * Sets a JSON encoded response. The response's {@code Content-Type} header will be set to {@code application/json}
     * @param response The response object
     * @param encoded The JSON-encoded string
     */
    public static void makeJsonResponse(HttpResponse response, String encoded) {
        StringEntity ent = new StringEntity(encoded, ContentType.APPLICATION_JSON);
        response.setEntity(ent);
    }

    /**
     * Send and flush the response object over the current connection and close the connection
     * @param cx The context
     * @param response The response object
     * @throws IOException
     * @throws HttpException
     */
    public static void bailResponse(HttpContext cx, HttpResponse response) throws IOException, HttpException {
        HttpServerConnection conn = getConnection(cx);
        conn.sendResponseHeader(response);
        conn.sendResponseEntity(response);
        conn.flush();
    }

    /**
     * Get any authorization credentials supplied over the connection. If no credentials were provided in the request,
     * an empty AuthContex is returned
     * @param cx The HTTP Context (from httpcomonents)
     * @param req The request
     * @return The authentication info
     * @throws IOException
     */
    public static AuthContext getAuth(HttpContext cx, HttpRequest req) throws IOException {
        AuthContext auth = (AuthContext) cx.getAttribute(HttpServer.CX_AUTH);
        if (auth == null) {
            Header authHdr = req.getLastHeader(HttpHeaders.AUTHORIZATION);
            if (authHdr == null) {
                auth = new AuthContext();
            } else {
                auth = new AuthContext(authHdr.getValue());
            }
        }
        return auth;
    }
}
