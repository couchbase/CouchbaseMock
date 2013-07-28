/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.http;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.*;
import java.net.URLDecoder;
import org.couchbase.mock.harakiri.CommandNotFoundException;
import org.couchbase.mock.harakiri.HarakiriCommand;
import org.couchbase.mock.harakiri.HarakiriDispatcher;
import org.couchbase.mock.harakiri.HarakiriHelpCommand;
import org.couchbase.mock.harakiri.HarakiriDispatcher.PayloadFormat;
/**
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class ControlHandler implements HttpHandler {
    private final HarakiriDispatcher dispatcher;

    private static class InvalidQueryException extends Exception { }
    private class WantHelpException extends Exception { }

    public ControlHandler(HarakiriDispatcher dispatcher) {
        this.dispatcher = dispatcher;

    }


    private static JsonObject parseQueryParams(HttpExchange exchange) throws InvalidQueryException {

        String query = exchange.getRequestURI().getRawQuery();
        JsonObject payload = new JsonObject();
        JsonParser parser = new JsonParser();

        if (query == null) {
            throw new InvalidQueryException();
        }

        for (String kv : query.split("&")) {
            String[] parts = kv.split("=");

            if (parts.length != 2) {
                throw new InvalidQueryException();
            }

            String optName = parts[0];
            JsonElement optVal;
            try {
                 optVal = parser.parse(URLDecoder.decode(parts[1], "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new InvalidQueryException();
            }

            payload.add(optName, optVal);
        }

        return payload;
    }

    /**
     * Sends a help text with the provided code
     * @param exchange The exchange to send the text to
     * @param code The HTTP status code to be used in the response
     */
    private static void sendHelpText(HttpExchange exchange, int code) throws IOException {

        byte[] ret = HarakiriHelpCommand.getIndentedHelp().getBytes();
        exchange.sendResponseHeaders(code, ret.length);
        exchange.getResponseBody().write(ret);
    }


    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            _handle(exchange);

        } catch (IOException e) {
            throw e;

        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }


    private void _handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        path = path.replaceFirst("^/+", "");
        String[] components = path.split("/");
        OutputStream body = exchange.getResponseBody();
        exchange.getResponseHeaders().set("Content-Type", "application/json");

        try {

            if (components.length != 2) {
                throw new CommandNotFoundException("/mock/<COMMAND>", null);
            }

            if (components[1].equals("help")) {
                throw new WantHelpException();
            }

            JsonObject payload = parseQueryParams(exchange);
            String cmdStr = components[1];

            HarakiriCommand cmd = dispatcher.getCommand(
                    PayloadFormat.JSON, cmdStr, payload);

            byte[] response = cmd.getResponse().getBytes();

            exchange.sendResponseHeaders(200, response.length);
            body.write(response);

        } catch (WantHelpException e) {
            sendHelpText(exchange, 200);

        } catch (CommandNotFoundException e) {
            sendHelpText(exchange, 404);

        } catch (InvalidQueryException e) {
            sendHelpText(exchange, 400);

        } catch (RuntimeException e) {
            exchange.sendResponseHeaders(500, -1);
            e.printStackTrace(new PrintWriter(body));
            throw e;

        } finally {
            body.close();
        }
    }
}
