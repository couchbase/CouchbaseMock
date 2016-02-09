/*
 * Copyright 2015 Couchbase, Inc.
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

package org.couchbase.mock.client;

import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.client.*;
import org.couchbase.mock.memcached.protocol.*;
import org.couchbase.mock.memcached.client.CommandBuilder.MultiMutationSpec;
import org.couchbase.mock.memcached.client.CommandBuilder.MultiLookupSpec;

import java.util.List;

public class ClientSubdocTest extends ClientBaseTest {
    private MemcachedClient client;
    private short vbId;
    private final static String docId = "someKey";
    private final static String multiDocId = "multiKey";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        client = getBinClient(0);
        vbId = findValidVbucket(0);

        // Store the item
        ClientResponse resp = client.sendRequest(CommandBuilder.buildStore(docId, vbId, "{}"));
        assertTrue(resp.success());

        String multiValue = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
        resp = client.sendRequest(CommandBuilder.buildStore(multiDocId, vbId, multiValue));
        assertTrue(resp.success());
    }

    public void testSubdocBasic() throws Exception {
        ClientResponse resp;
        CommandBuilder cb;

        resp = client.sendRequest(CommandBuilder.buildSubdocGet(docId, vbId, "path"));
        assertEquals(ErrorCode.SUBDOC_PATH_ENOENT, resp.getStatus());

        // Do an upsert
        cb = new CommandBuilder(CommandCode.SUBDOC_DICT_ADD)
                .key(docId, vbId)
                .subdoc("hello".getBytes(), "\"world\"".getBytes());

        resp = client.sendRequest(cb);
        assertTrue(resp.success());
        long cas = resp.getCas();
        assertFalse(cas == 0);

        // Try it again (should fail with PATH_EEXISTS)
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_PATH_EEXISTS, resp.getStatus());

        // Get it back
        resp = client.sendRequest(CommandBuilder.buildSubdocGet(docId, vbId, "hello"));
        assertTrue(resp.success());
        assertEquals("\"world\"", resp.getValue());
        assertEquals(resp.getCas(), cas);

        // Remove it
        byte[] req = new CommandBuilder(CommandCode.SUBDOC_DELETE)
                .key(docId, vbId).subdoc("hello".getBytes()).build();

        resp = client.sendRequest(req);
        assertTrue(resp.success());
        assertFalse(cas == resp.getCas());
        assertFalse(resp.getCas() == 0);

        // Get it again
        resp = client.sendRequest(CommandBuilder.buildSubdocGet(docId, vbId, "hello"));
        assertEquals(ErrorCode.SUBDOC_PATH_ENOENT, resp.getStatus());
    }

    public void testCas() throws Exception {
        ClientResponse resp = client.sendRequest(
                new CommandBuilder(CommandCode.GET).key(docId, vbId));
        assertTrue(resp.success());

        long cas = resp.getCas();
        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_DICT_ADD)
                .key(docId, vbId)
                .subdoc("foo".getBytes(), "123".getBytes())
                .cas(cas + 1);

        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.KEY_EEXISTS, resp.getStatus());

        cb.cas(cas);
        resp = client.sendRequest(cb);
        assertTrue(resp.success());
    }

    public void testExpiry() throws Exception {
        Item existing = getItem(docId, vbId);
        assertEquals(0, existing.getExpiryTime());

        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_DICT_UPSERT)
                .key(docId, vbId)
                .subdoc("foo".getBytes(), "123".getBytes(), 0, 30);
        ClientResponse resp = client.sendRequest(cb);
        assertTrue(resp.success());

        // Get the item again
        existing = getItem(docId, vbId);
        assertTrue(existing.getExpiryTime() != 0);

        // Reset expiration time
        cb.subdoc("foo".getBytes(), "123".getBytes());
        resp = client.sendRequest(cb);
        assertTrue(resp.success());
        existing = getItem(docId, vbId);
        assertEquals(0, existing.getExpiryTime());
    }

    public void testInvalid() throws Exception {
        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_DICT_UPSERT)
                .key(docId, vbId)
                .subdoc("bad..path[]".getBytes(), "123".getBytes());
        ClientResponse resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_PATH_EINVAL, resp.getStatus());

        cb.subdoc("path".getBytes(), "non-json-value".getBytes());
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_VALUE_CANTINSERT, resp.getStatus());

        cb.subdoc("[0]".getBytes(), "123".getBytes());
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_PATH_MISMATCH, resp.getStatus());
    }

    public void testEmptyPath() throws Exception {
        byte[] req = CommandBuilder.buildSubdocGet(docId, vbId, "");
        ClientResponse resp = client.sendRequest(req);
        assertTrue(resp.success());
        assertEquals("{}", resp.getValue());
    }

    public void testCounter() throws Exception {
        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_COUNTER)
                .key(docId, vbId)
                .subdoc("counter", "42");
        ClientResponse resp = client.sendRequest(cb);
        assertTrue(resp.success());
        assertEquals("42", resp.getValue());

        // Try it again
        resp = client.sendRequest(cb);
        assertTrue(resp.success());
        assertEquals("84", resp.getValue());

        // Try with a large value
        CommandBuilder storeCb = new CommandBuilder(CommandCode.SUBDOC_DICT_UPSERT)
                .key(docId, vbId)
                .subdoc("counter", "9999999999999999999999999999999999999999999999");
        resp = client.sendRequest(storeCb);
        assertTrue(resp.success());
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_NUM_ERANGE, resp.getStatus());

        // Test with an invalid number
        storeCb = new CommandBuilder(CommandCode.SUBDOC_COUNTER)
                .key(docId, vbId)
                .subdoc("counter", "bad number");
        resp = client.sendRequest(storeCb);
        assertEquals(ErrorCode.SUBDOC_DELTA_ERANGE, resp.getStatus());
    }

    public void testMultiLookups() throws Exception {
        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_MULTI_LOOKUP)
                .key(multiDocId, vbId)
                .subdocMultiLookup(
                        MultiLookupSpec.get("key1"),
                        MultiLookupSpec.get("key2"),
                        MultiLookupSpec.get("key3"))
                ;
        ClientResponse resp = client.sendRequest(cb);
        assertTrue(resp.success());
        List<MultiLookupResult> results = MultiLookupResult.parse(resp.getRawValue());
        assertEquals(3, results.size());
        assertEquals("\"value1\"", results.get(0).getValue());
        assertEquals("\"value2\"", results.get(1).getValue());
        assertEquals("\"value3\"", results.get(2).getValue());
        for (MultiLookupResult res : results) {
            assertTrue(res.success());
        }

        // Do the same thing, but with exists..
        cb = new CommandBuilder(CommandCode.SUBDOC_MULTI_LOOKUP)
                .key(multiDocId, vbId)
                .subdocMultiLookup(
                        MultiLookupSpec.exists("key1"),
                        MultiLookupSpec.exists("key2"),
                        MultiLookupSpec.exists("key3")
                );
        resp = client.sendRequest(cb);
        assertTrue(resp.success());
        results = MultiLookupResult.parse(resp.getRawValue());
        assertEquals(3, results.size());
        for (MultiLookupResult res : results) {
            assertTrue(res.getValue().isEmpty());
            assertTrue(res.success());
        }

        // Test mixed mode
        cb = new CommandBuilder(CommandCode.SUBDOC_MULTI_LOOKUP)
                .key(multiDocId, vbId)
                .subdocMultiLookup(
                        MultiLookupSpec.get("key1"),
                        MultiLookupSpec.get("non-exist"),
                        MultiLookupSpec.get("key2")
                );
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_MULTI_FAILURE, resp.getStatus());
        results = MultiLookupResult.parse(resp.getRawValue());

        // First one is OK
        assertTrue(results.get(0).success());
        assertEquals("\"value1\"", results.get(0).getValue());

        // Second one is bad
        assertEquals(ErrorCode.SUBDOC_PATH_ENOENT, results.get(1).getStatus());
        assertTrue(results.get(1).getValue().isEmpty());

        // Third is also OK
        assertTrue(results.get(2).success());
        assertEquals("\"value2\"", results.get(2).getValue());

        cb.key("nonExist", vbId);
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.KEY_ENOENT, resp.getStatus());
        results = MultiLookupResult.parse(resp.getRawValue());
        assertTrue(results.isEmpty());
    }

    public void testMultiMutations() throws Exception {
        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_MULTI_MUTATION)
                .key(multiDocId, vbId)
                .subdocMultiMutation(
                        new MultiMutationSpec(CommandCode.SUBDOC_DICT_ADD, "new1", "\"v1\""),
                        new MultiMutationSpec(CommandCode.SUBDOC_DICT_UPSERT, "new2", "\"v2\""),
                        new MultiMutationSpec(CommandCode.SUBDOC_DICT_ADD, "new3", "\"v3\"")
                );
        ClientResponse resp = client.sendRequest(cb);
        assertTrue(resp.success());

        cb.subdocMultiMutation(
                new MultiMutationSpec(CommandCode.SUBDOC_DICT_UPSERT, "new4", "\"v4\""),
                new MultiMutationSpec(CommandCode.SUBDOC_DICT_ADD, "new1", "\"badv1\""),
                new MultiMutationSpec(CommandCode.SUBDOC_COUNTER, "counterVal", "42", true)
        );

        // Try with an error. Sending it again should be enough
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_MULTI_FAILURE, resp.getStatus());
        List<MultiMutationResult> res = MultiMutationResult.parse(resp.getRawValue());
        assertEquals(1, res.size());
        assertEquals(ErrorCode.SUBDOC_PATH_EEXISTS, res.get(0).getStatus());
        assertEquals(1, res.get(0).getIndex());


        // Test with counters..
        cb.subdocMultiMutation(
                new MultiMutationSpec(CommandCode.SUBDOC_COUNTER, "counter1", "50"),
                new MultiMutationSpec(CommandCode.SUBDOC_DICT_UPSERT, "newField", "\"newValue\""),
                new MultiMutationSpec(CommandCode.SUBDOC_ARRAY_PUSH_FIRST, "newArray", "123", true),
                new MultiMutationSpec(CommandCode.SUBDOC_COUNTER, "counter2", "100")
        );
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUCCESS, resp.getStatus());
        res = MultiMutationResult.parse(resp.getRawValue());
        assertEquals(2, res.size());

        assertEquals(0, res.get(0).getIndex());
        assertEquals("50", res.get(0).getValue());
        assertEquals(ErrorCode.SUCCESS, res.get(0).getStatus());

        assertEquals(3, res.get(1).getIndex());
        assertEquals("100", res.get(1).getValue());
        assertEquals(ErrorCode.SUCCESS, res.get(1).getStatus());
    }

    public void testInvalidMultiCombos() throws Exception {
        CommandBuilder cb = new CommandBuilder(CommandCode.SUBDOC_MULTI_MUTATION)
                .key(multiDocId, vbId);
        cb.subdocMultiMutation(
                new MultiMutationSpec(CommandCode.SUBDOC_GET, "blah"),
                new MultiMutationSpec(CommandCode.SUBDOC_DICT_ADD, "new1")
        );
        ClientResponse resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_INVALID_COMBO, resp.getStatus());

        // Test with invalid lookup specs
        cb = new CommandBuilder(CommandCode.SUBDOC_MULTI_LOOKUP)
                .key(multiDocId, vbId)
                .subdocMultiLookup(
                        new MultiLookupSpec(CommandCode.SUBDOC_ARRAY_ADD_UNIQUE, "blah"),
                        new MultiLookupSpec(CommandCode.SUBDOC_DELETE, "argh")
                );
        resp = client.sendRequest(cb);
        assertEquals(ErrorCode.SUBDOC_INVALID_COMBO, resp.getStatus());
    }
}
