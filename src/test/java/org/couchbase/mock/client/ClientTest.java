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

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ObserveResponse;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class ClientTest extends ClientBaseTest {

    public void testSimpleGetSet() throws Exception {
        String key = "key";
        String value = "value";
        OperationFuture<Boolean> f = client.set("key", value);
        assertTrue(f.get());
        assertTrue(f.getCas() > 0);

        Object ret = client.get(key);
        assertNotNull(ret);
        assertTrue(String.class.isInstance(ret));
        assertEquals(value, (String)ret);

        assertNull(client.get("non-exist-key"));
        assertTrue(client.delete(key).get());
        assertNull(client.get("key"));
    }

    public void testBulkOperations() throws Exception {
        Set<String> keyList = new HashSet<String>();
        Map<String,Long> casMap = new HashMap<String, Long>();
        List<OperationFuture> futures = new ArrayList<OperationFuture>();

        for (int ii = 0; ii < 100; ii++) {
            String s = "Key_" + ii;
            keyList.add(s);
            futures.add(client.set(s, s));
        }

        for (OperationFuture f : futures) {
            f.get();
            assertTrue(f.getStatus().isSuccess());
            long cas = f.getCas();
            assertTrue(keyList.contains(f.getKey()));
            assertTrue(cas != 0);
            casMap.put(f.getKey(), cas);
        }

        Map<String,Object> bulkResult = client.getBulk(keyList);
        assertEquals(bulkResult.size(), keyList.size());
        for (Map.Entry<String,Object> kv : bulkResult.entrySet()) {
            assertTrue(keyList.contains(kv.getKey()));
            assertEquals(kv.getKey(), kv.getValue());
            OperationFuture<CASValue<Object>> gFuture = client.asyncGets(kv.getKey());
            assertTrue(gFuture.get().getCas() == casMap.get(kv.getKey()));
        }

        futures.clear();
        for (String s : keyList) {
            futures.add(client.delete(s));
        }

        for (OperationFuture f : futures) {
            f.get();
            assertTrue(f.getStatus().isSuccess());
            assertNull(client.get(f.getKey()));
        }
    }

    public void testBasicObserve() throws Exception {
        // 4 Nodes, 2 Replicas
        String key = "observe key";
        OperationFuture ft = client.set(key, key);
        ft.get();
        Map<MemcachedNode, ObserveResponse> observeResponseMap = client.observe(key, ft.getCas());
        assertEquals(bucketConfiguration.numReplicas + 1, observeResponseMap.size());
        boolean foundMaster = false;
        MemcachedNode master = getMasterForKey(key);
        for (Map.Entry<MemcachedNode,ObserveResponse> kv : observeResponseMap.entrySet()) {
            byte resp = kv.getValue().getResponse();
            client.observePoll(key, resp, PersistTo.ZERO, ReplicateTo.ZERO, foundMaster);
            assertEquals(ObserveResponse.FOUND_PERSISTED, ObserveResponse.valueOf(resp));
            if (kv.getKey() == master) {
                foundMaster = true;
            }
        }
        assertTrue(foundMaster);
    }

    public void testTTL() throws Exception {
        String key = "ttl_key";
        OperationFuture ft = client.set(key, 1, key);
        ft.get();
        assertTrue(mockClient.request(new TimeTravelRequest(2)).isOk());
        assertNull(client.get(key));
        ft = client.set(key, 10, key);
        ft.get();
        assertNotNull(client.get(key));
    }

    public void testAppend() throws Exception {
        String baseStr = "MIDDLE";
        String beginStr = "BEGIN_";
        String endStr = "_END";
        String key = "append_key";

        assertTrue(client.set(key, baseStr).get());
        assertEquals(baseStr, client.get(key));

        assertTrue(client.append(key, endStr).get());
        assertEquals(baseStr + endStr, client.get(key));

        assertTrue(client.prepend(key, beginStr).get());
        assertEquals(beginStr + baseStr + endStr, client.get(key));

        assertTrue(client.delete(key).get());
        OperationFuture ft = client.append(key, "blah blah");
        ft.get();
        assertFalse(ft.getStatus().isSuccess());
    }

    public void testKeyStats() throws Exception {
        client.set("foo", "bar");
        OperationFuture<Map<String,String>> ft;
        ft = client.getKeyStats("foo");

        Map<String,String> results = ft.get();
        assertTrue(ft.getStatus().isSuccess());

        assertTrue(results.containsKey("key_is_dirty"));
        assertTrue(results.containsKey("key_exptime"));
        assertTrue(results.containsKey("key_flags"));
        assertTrue(results.containsKey("key_cas"));
        assertTrue(results.containsKey("key_vb_state"));
        assertEquals("active", results.get("key_vb_state"));
        assertEquals("0", results.get("key_exptime"));

        client.delete("foo").get();
        ft = client.getKeyStats("foo");
        ft.get();
        assertFalse(ft.getStatus().isSuccess());
    }

    public void testBasicStats() throws Exception {
        assertFalse(client.getStats().isEmpty());
    }
}
