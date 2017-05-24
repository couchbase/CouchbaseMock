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

package com.couchbase.mock.client;

import com.couchbase.mock.memcached.client.ClientResponse;
import com.couchbase.mock.memcached.client.CommandBuilder;
import com.couchbase.mock.memcached.client.MemcachedClient;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.ErrorCode;

import java.nio.ByteBuffer;

/**
 * Test that various binary level functionality works as expected
 * @author Mark Nunberg
 */
public class SaslAuthTest extends ClientBaseTest {
    @Override
    protected void setUp() throws Exception {
        createMock("protected", "secret");
    }

    private byte[] buildPLAIN(String id, String user, String pass) {
        ByteBuffer buf = ByteBuffer.allocate(id.length()+user.length()+pass.length() + 2);
        buf.put(id.getBytes());
        buf.put((byte)0x00);
        buf.put(user.getBytes());
        buf.put((byte)0x00);
        buf.put(pass.getBytes());
        return buf.array();
    }

    public void testSaslAuth() throws Exception {
        MemcachedClient binClient = getBinClient(0);
        CommandBuilder cBuilder = new CommandBuilder(CommandCode.SASL_AUTH);
        ClientResponse resp;

        cBuilder.value(buildPLAIN("foo", "protected", "secret"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.SUCCESS, resp.getStatus());

        // Try with a bad password
        cBuilder.value("blah");
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        // Missing ID; still have a NUL
        cBuilder.value(buildPLAIN("", "protected", "secret"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.SUCCESS, resp.getStatus());

        // Bad password
        cBuilder.value(buildPLAIN("id", "protected", "bad"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        // Empty
        cBuilder.value(buildPLAIN("", "", ""));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        // Empty password
        cBuilder.value(buildPLAIN("foo", "protected", ""));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        cBuilder.value(buildPLAIN("foo", "", "secret"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        binClient.close();

    }
}
