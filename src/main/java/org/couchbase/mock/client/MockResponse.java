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
package org.couchbase.mock.client;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author mnunberg
 */
public class MockResponse {
    private static final Logger logger = Logger.getLogger(MockResponse.class.getName());
    private final JsonObject response;

    public MockResponse(String jsonString) {
        response = new Gson().fromJson(jsonString, JsonObject.class);
    }

    public boolean isOk() {
        boolean result = response.get("status").getAsString().toLowerCase().equals("ok");
        if (!result) {
          logger.fine("Response : " + response.get("status").getAsString());
          logger.fine("Error : " + getErrorMessage());
        }
        return result;
    }

    public String getErrorMessage() {
        if (response.has("error")) {
            return response.get("error").getAsString();
        } else {
            return "";
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public JsonElement getPayload() {
        return response.get("payload");
    }

    @SuppressWarnings("UnusedDeclaration")
    public JsonObject getRawJson() {
        return response;
    }
}
