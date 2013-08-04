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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.Map;

/**
 * The MockHttpClient is an implementation of the AbstractMockClient that
 * communicates with the Mock server over HTTP.
 */
public class MockHttpClient extends AbstractMockClient {
    private final InetSocketAddress restAddress;

    /**
     * Create a new HTTP Mock command client. This interacts with the mock
     * over the REST api using the special '/mock' path
     *
     * @param restAddress The address of the REST server
     */
    public MockHttpClient(@NotNull InetSocketAddress restAddress) {
        this.restAddress = restAddress;
    }

    private URL buildRequestUri(MockRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append("http://")
                .append(this.restAddress.getHostName())
                .append(":")
                .append(this.restAddress.getPort())
                .append("/mock/")
                .append(URLEncoder.encode(request.getName(), "UTF-8"))
                .append("?");
        appendPayload(sb, request.getPayload());
        return new URL(sb.toString());
    }

    private void appendPayload(StringBuilder sb, Map<String, Object> payload) throws UnsupportedEncodingException {
        Gson gson = new Gson();
        for (Map.Entry<String, Object> kv : payload.entrySet()) {
            String jStr = gson.toJson(kv.getValue());
            jStr = URLEncoder.encode(jStr, "UTF-8");
            sb.append(kv.getKey()).append('=').append(jStr).append('&');
        }
        int index = sb.lastIndexOf("&");
        if (index > 0) {
            sb.deleteCharAt(index);
        }
    }

    @NotNull
    @Override
    public MockResponse request(@NotNull MockRequest request) throws IOException {
        URL url = buildRequestUri(request);
        HttpURLConnection uc = (HttpURLConnection) url.openConnection();
        uc.connect();
        return new MockResponse(readResponse(uc.getInputStream()));
    }

    private String readResponse(@NotNull InputStream input) throws IOException {
        StringBuilder sb = new StringBuilder();
        byte[] buf = new byte[4096];
        int nr;
        while ((nr = input.read(buf)) != -1) {
            sb.append(new String(buf, 0, nr));
        }
        return sb.toString();
    }
}