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

/**
 * Created by mnunberg on 12/17/14.
 */
public class HandlerUtil {
    private HandlerUtil() {}

    public static HttpServerConnection getConnection(HttpContext cx) {
        return (HttpServerConnection) cx.getAttribute(HttpCoreContext.HTTP_CONNECTION);
    }

    public static Socket getSocket(HttpContext cx) {
        return (Socket) cx.getAttribute(HttpServer.CX_SOCKET);
    }

    public static URL getUrl(HttpRequest request) throws MalformedURLException{
        String uriStr = request.getRequestLine().getUri();
        try {
            return URI.create(uriStr).toURL();
        } catch (IllegalArgumentException ex) {
            return URI.create("http://dummy/" + uriStr).toURL();
        }
    }

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

    public static void makeStringResponse(HttpResponse response, String s) {
        StringEntity entity = new StringEntity(s, ContentType.TEXT_PLAIN);
        entity.setContentEncoding("utf-8");
        response.setEntity(entity);
    }

    public static void makeJsonResponse(HttpResponse response, String encoded) {
        StringEntity ent = new StringEntity(encoded, ContentType.APPLICATION_JSON);
        response.setEntity(ent);
    }
    public static void bailResponse(HttpContext cx, HttpResponse response) throws IOException, HttpException {
        HttpServerConnection conn = getConnection(cx);
        conn.sendResponseHeader(response);
        conn.sendResponseEntity(response);
        conn.flush();
    }

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
