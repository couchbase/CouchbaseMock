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

import java.util.HashMap;
import java.util.Map;

/**
 * @author mnunberg
 */
public class MockRequest {
    protected final Map<String, Object> command = new HashMap<String, Object>();
    protected final Map<String, Object> payload = new HashMap<String, Object>();

    public void setName(String name) {
        command.put("command", name);
    }

    public String getName() {
        return (String) command.get("command");
    }

    public void set(String param, Object value) {
        payload.put(param, value);
    }

    public void clear() {
        command.clear();
        payload.clear();
        command.put("payload", payload);
    }

    Map<String, Object> getMap() {
        return command;
    }

    Map<String, Object> getPayload() {
        return payload;
    }

    public MockRequest() {
        command.put("payload", payload);
    }

    public String getEncodedJson() {
        return new Gson().toJson(command);
    }

    public static MockRequest build(String name, Object... kv) {
        if (kv.length % 2 != 0) {
            throw new IllegalArgumentException("Variable params must be even");
        }

        MockRequest request = new MockRequest();
        request.setName(name);
        for (int ii = 0; ii < kv.length; ii += 2) {
            request.set(kv[ii].toString(), kv[ii + 1]);
        }
        return request;
    }
}