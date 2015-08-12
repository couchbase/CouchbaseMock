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
import com.google.gson.JsonObject;
import java.io.*;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.couchbase.mock.control.CommandNotFoundException;
import org.couchbase.mock.control.MockCommandDispatcher;
import org.couchbase.mock.control.handlers.MockHelpCommandHandler;
import org.couchbase.mock.httpio.HandlerUtil;

/**
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class ControlHandler implements HttpRequestHandler {
    private final MockCommandDispatcher dispatcher;
    private class WantHelpException extends Exception { }

    public ControlHandler(MockCommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;

    }
    /**
     * Sends a help text with the provided code
     * @param response The response
     * @param code The HTTP status code to be used in the response
     */
    private static void sendHelpText(HttpResponse response, int code) throws IOException {
        HandlerUtil.makeStringResponse(response, MockHelpCommandHandler.getIndentedHelp());
        response.setStatusCode(code);
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        URL url = HandlerUtil.getUrl(request);
        String path = url.getPath();
        path = path.replaceFirst("^/+", "");
        String[] components = path.split("/");

        try {

            if (components.length != 2) {
                throw new CommandNotFoundException("/mock/<COMMAND>", null);
            }

            if (components[1].equals("help")) {
                throw new WantHelpException();
            }

            JsonObject payload = HandlerUtil.getJsonQuery(url);
            String cmdStr = URLDecoder.decode(components[1], "UTF-8");
            String rStr = dispatcher.dispatch(cmdStr, payload).toString();
            HandlerUtil.makeJsonResponse(response, rStr);

        } catch (WantHelpException e) {
            sendHelpText(response, 200);

        } catch (CommandNotFoundException e) {
            sendHelpText(response, 404);

        } catch (RuntimeException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(new PrintWriter(pw));
            HandlerUtil.makeStringResponse(response, sw.toString());
            HandlerUtil.bailResponse(context, response);
            throw e;

        }
    }
}
