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
import  org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * The AbstractMockClient is an abstract class used to perform operations on the
 * mock server. The Mock server provides two mechanisms to communicate
 * to the mock server (over a dedicated connection or over HTTP).
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractMockClient {
    /**
     * Negotiates the connection between the client and the Mock server.
     *
     * @throws IOException
     */
    public void negotiate() throws IOException {
    }

    /**
     * Sends a mock command to the mock
     *
     * @param req The command to send
     * @return The response received
     */
    public abstract @NotNull MockResponse request(@NotNull MockRequest req) throws IOException;

    /**
     * Closes the mock connection
     */
    public void shutdown() {
    }
}
