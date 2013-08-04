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
import java.util.ArrayList;

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
        assertTrue(mockClient.request(new EndureRequest("key", "value", true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new EndureRequest("key", "value", true, 2, "default")).isOk());
        ArrayList<Integer> replicaId = new ArrayList<Integer>();
        for (int ii = 0; ii < bucketConfiguration.numReplicas; ++ii) {
            replicaId.add(ii);
        }
        assertTrue(mockClient.request(new EndureRequest("key", "value", true, replicaId)).isOk());
        assertTrue(mockHttpClient.request(new EndureRequest("key", "value", true, replicaId)).isOk());
        assertTrue(mockClient.request(new EndureRequest("key", "value", true, replicaId, "default")).isOk());
        assertTrue(mockHttpClient.request(new EndureRequest("key", "value", true, replicaId, "default")).isOk());
        // Now verify that we can't endure over the max number..
        replicaId.add(bucketConfiguration.numReplicas);
        assertFalse(mockClient.request(new EndureRequest("key", "value", true, replicaId)).isOk());
        assertFalse(mockClient.request(new EndureRequest("key", "value", true, replicaId, "default")).isOk());
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

    public void testTruncate() throws IOException {
        assertTrue(mockClient.request(new TruncateRequest(1000)).isOk());
        assertTrue(mockHttpClient.request(new TruncateRequest(10)).isOk());
    }

    public void testPersist() throws IOException {
        assertTrue(mockClient.request(new PersistRequest("key", "value", 0, true, 0)).isOk());
        assertTrue(mockHttpClient.request(new PersistRequest("key", "value", 0, true, 1)).isOk());
        assertTrue(mockClient.request(new PersistRequest("key", "value", 0, true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new PersistRequest("key", "value", 0, true, 2, "default")).isOk());
        ArrayList<Integer> replicaId = new ArrayList<Integer>();
        for (int ii = 0; ii < bucketConfiguration.numReplicas; ++ii) {
            replicaId.add(ii);
        }
        assertTrue(mockClient.request(new PersistRequest("key", "value", 0, true, replicaId)).isOk());
        assertTrue(mockHttpClient.request(new PersistRequest("key", "value", 0, true, replicaId)).isOk());
        assertTrue(mockClient.request(new PersistRequest("key", "value", 0, true, replicaId, "default")).isOk());
        assertTrue(mockHttpClient.request(new PersistRequest("key", "value", 0, true, replicaId, "default")).isOk());
        // Now verify that we can't persist over the max number..
        replicaId.add(bucketConfiguration.numReplicas);
        assertFalse(mockClient.request(new PersistRequest("key", "value", 0, true, replicaId)).isOk());
        assertFalse(mockClient.request(new PersistRequest("key", "value", 0, true, replicaId, "default")).isOk());

        // try to set with random cas
        assertTrue(mockClient.request(new PersistRequest("key", "value", 123, true, 0)).isOk());
        assertTrue(mockHttpClient.request(new PersistRequest("key", "value", 456, true, 1)).isOk());
        assertTrue(mockClient.request(new PersistRequest("key", "value", 789, true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new PersistRequest("key", "value", 123, true, 2, "default")).isOk());
    }

    public void testUnpersist() throws IOException {
        assertTrue(mockClient.request(new UnpersistRequest("key", true, 0)).isOk());
        assertTrue(mockHttpClient.request(new UnpersistRequest("key", true, 1)).isOk());
        assertTrue(mockClient.request(new UnpersistRequest("key", true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new UnpersistRequest("key", true, 2, "default")).isOk());
        ArrayList<Integer> replicaId = new ArrayList<Integer>();
        for (int ii = 0; ii < bucketConfiguration.numReplicas; ++ii) {
            replicaId.add(ii);
        }
        assertTrue(mockClient.request(new UnpersistRequest("key", true, replicaId)).isOk());
        assertTrue(mockHttpClient.request(new UnpersistRequest("key", true, replicaId)).isOk());
        assertTrue(mockClient.request(new UnpersistRequest("key", true, replicaId, "default")).isOk());
        assertTrue(mockHttpClient.request(new UnpersistRequest("key", true, replicaId, "default")).isOk());
        // Now verify that we can't unpersist over the max number..
        replicaId.add(bucketConfiguration.numReplicas);
        assertFalse(mockClient.request(new UnpersistRequest("key", true, replicaId)).isOk());
        assertFalse(mockClient.request(new UnpersistRequest("key", true, replicaId, "default")).isOk());
    }

    public void testCache() throws IOException {
        assertTrue(mockClient.request(new CacheRequest("key", "value", 0, true, 0)).isOk());
        assertTrue(mockHttpClient.request(new CacheRequest("key", "value", 0, true, 1)).isOk());
        assertTrue(mockClient.request(new CacheRequest("key", "value", 0, true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new CacheRequest("key", "value", 0, true, 2, "default")).isOk());
        ArrayList<Integer> replicaId = new ArrayList<Integer>();
        for (int ii = 0; ii < bucketConfiguration.numReplicas; ++ii) {
            replicaId.add(ii);
        }
        assertTrue(mockClient.request(new CacheRequest("key", "value", 0, true, replicaId)).isOk());
        assertTrue(mockHttpClient.request(new CacheRequest("key", "value", 0, true, replicaId)).isOk());
        assertTrue(mockClient.request(new CacheRequest("key", "value", 0, true, replicaId, "default")).isOk());
        assertTrue(mockHttpClient.request(new CacheRequest("key", "value", 0, true, replicaId, "default")).isOk());
        // Now verify that we can't cache over the max number..
        replicaId.add(bucketConfiguration.numReplicas);
        assertFalse(mockClient.request(new CacheRequest("key", "value", 0, true, replicaId)).isOk());
        assertFalse(mockClient.request(new CacheRequest("key", "value", 0, true, replicaId, "default")).isOk());

        // try to set with random cas
        assertTrue(mockClient.request(new CacheRequest("key", "value", 123, true, 0)).isOk());
        assertTrue(mockHttpClient.request(new CacheRequest("key", "value", 456, true, 1)).isOk());
        assertTrue(mockClient.request(new CacheRequest("key", "value", 789, true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new CacheRequest("key", "value", 123, true, 2, "default")).isOk());
    }

    public void testUncache() throws IOException {
        assertTrue(mockClient.request(new UncacheRequest("key", true, 0)).isOk());
        assertTrue(mockHttpClient.request(new UncacheRequest("key", true, 1)).isOk());
        assertTrue(mockClient.request(new UncacheRequest("key", true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new UncacheRequest("key", true, 2, "default")).isOk());
        ArrayList<Integer> replicaId = new ArrayList<Integer>();
        for (int ii = 0; ii < bucketConfiguration.numReplicas; ++ii) {
            replicaId.add(ii);
        }
        assertTrue(mockClient.request(new UncacheRequest("key", true, replicaId)).isOk());
        assertTrue(mockHttpClient.request(new UncacheRequest("key", true, replicaId)).isOk());
        assertTrue(mockClient.request(new UncacheRequest("key", true, replicaId, "default")).isOk());
        assertTrue(mockHttpClient.request(new UncacheRequest("key", true, replicaId, "default")).isOk());
        // Now verify that we can't uncache over the max number..
        replicaId.add(bucketConfiguration.numReplicas);
        assertFalse(mockClient.request(new UncacheRequest("key", true, replicaId)).isOk());
        assertFalse(mockClient.request(new UncacheRequest("key", true, replicaId, "default")).isOk());
    }

    public void testPurge() throws IOException {
        assertTrue(mockClient.request(new PurgeRequest("key", true, 0)).isOk());
        assertTrue(mockHttpClient.request(new PurgeRequest("key", true, 1)).isOk());
        assertTrue(mockClient.request(new PurgeRequest("key", true, 2, "default")).isOk());
        assertTrue(mockHttpClient.request(new PurgeRequest("key", true, 2, "default")).isOk());
        ArrayList<Integer> replicaId = new ArrayList<Integer>();
        for (int ii = 0; ii < bucketConfiguration.numReplicas; ++ii) {
            replicaId.add(ii);
        }
        assertTrue(mockClient.request(new PurgeRequest("key", true, replicaId)).isOk());
        assertTrue(mockHttpClient.request(new PurgeRequest("key", true, replicaId)).isOk());
        assertTrue(mockClient.request(new PurgeRequest("key", true, replicaId, "default")).isOk());
        assertTrue(mockHttpClient.request(new PurgeRequest("key", true, replicaId, "default")).isOk());
        // Now verify that we can't purge over the max number..
        replicaId.add(bucketConfiguration.numReplicas);
        assertFalse(mockClient.request(new PurgeRequest("key", true, replicaId)).isOk());
        assertFalse(mockClient.request(new PurgeRequest("key", true, replicaId, "default")).isOk());
    }

    public void testKeyInfo() throws IOException {
        assertTrue(mockClient.request(new CacheRequest("key", "value", 123, true, 2)).isOk());
        assertTrue(mockClient.request(new KeyInfoRequest("key")).isOk());
        assertTrue(mockHttpClient.request(new KeyInfoRequest("key")).isOk());
        assertTrue(mockClient.request(new KeyInfoRequest("key", "default")).isOk());
        assertTrue(mockHttpClient.request(new KeyInfoRequest("key", "default")).isOk());
    }
}
