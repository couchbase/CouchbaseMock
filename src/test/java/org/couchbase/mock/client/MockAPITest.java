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

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Tests for the "extended" Mock API.
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class MockAPITest extends ClientBaseTest {
    MockHttpClient mockHttpClient;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mockHttpClient = new MockHttpClient(new InetSocketAddress("localhost", mockClient.getRestPort()));
    }

    public void testEndure() throws IOException {
        assertTrue(mockClient.request(new EndureRequest("key", "value", true, 2)).isOk());
        assertTrue(mockHttpClient.request(new EndureRequest("key", "value", true, 2)).isOk());
    }

    public void testTimeTravel() throws IOException {
        assertTrue(mockClient.request(new TimeTravelRequest(1)).isOk());
        assertTrue(mockClient.request(new TimeTravelRequest(-1)).isOk());

        MockResponse res = mockHttpClient.request(new TimeTravelRequest(1));
        if (!res.isOk()) {
            System.err.println(res.getErrorMessage());
        }

        assertTrue(mockHttpClient.request(new TimeTravelRequest(1)).isOk());
        assertTrue(mockHttpClient.request(new TimeTravelRequest(-1)).isOk());
    }

    public void testFailoverRespawn() throws IOException {
        assertTrue(mockClient.request(new FailoverRequest(1)).isOk());
        assertTrue(mockClient.request(new RespawnRequest(1)).isOk());
        assertTrue(mockHttpClient.request(new FailoverRequest(1)).isOk());
        assertTrue(mockHttpClient.request(new RespawnRequest(1)).isOk());
    }

    public void testHiccup() throws IOException {
        assertTrue(mockClient.request(new HiccupRequest(100, 10)).isOk());
        assertTrue(mockHttpClient.request(new HiccupRequest(1000, 10)).isOk());
    }
}
