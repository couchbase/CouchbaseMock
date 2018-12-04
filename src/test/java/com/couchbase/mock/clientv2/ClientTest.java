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

package com.couchbase.mock.clientv2;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.core.message.kv.subdoc.simple.SubCounterRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SubDictAddRequest;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.Info;
import com.couchbase.mock.client.MockClient;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientTest {
    protected final BucketConfiguration bucketConfiguration = new BucketConfiguration();
    protected MockClient mockClient;
    protected CouchbaseMock couchbaseMock;
    protected Cluster cluster;
    protected com.couchbase.client.java.Bucket bucket;
    protected int carrierPort;
    protected int httpPort;

    protected void getPortInfo(String bucket) throws Exception {
        httpPort = couchbaseMock.getHttpPort();
        carrierPort = couchbaseMock.getCarrierPort(bucket);
    }

    protected void createMock(@NotNull String name, @NotNull String password) throws Exception {
        bucketConfiguration.numNodes = 1;
        bucketConfiguration.numReplicas = 1;
        bucketConfiguration.numVBuckets = 1024;
        bucketConfiguration.name = name;
        bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
        bucketConfiguration.password = password;
        ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
        configList.add(bucketConfiguration);
        couchbaseMock = new CouchbaseMock(0, configList);
        couchbaseMock.start();
        couchbaseMock.waitForStartup();
    }

    protected void createClient() {
        cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder()
                .bootstrapCarrierDirectPort(carrierPort)
                .bootstrapHttpDirectPort(httpPort)
                .build(), "couchbase://127.0.0.1");
        bucket = cluster.openBucket("default");
    }

    @Before
    public void setUp() throws Exception {
        createMock("default", "");
        getPortInfo("default");
        createClient();
    }

    @After
    public void tearDown() {
        if (cluster != null) {
            cluster.disconnect();
        }
        if (couchbaseMock != null) {
            couchbaseMock.stop();
        }
        if (mockClient != null) {
            mockClient.shutdown();
        }
    }

    @Test
    public void testSimple() {
        bucket.upsert(JsonDocument.create("foo"));
        bucket.get(JsonDocument.create("foo"));
    }

    @Test
    public void testCasOnDelete() {
        JsonDocument doc = JsonDocument.create("foo", JsonObject.create().put("val", 42));
        JsonDocument upsertResult = bucket.upsert(doc);
        assertTrue(upsertResult.cas() > 0);
        JsonDocument deleteResult = bucket.remove("foo");
        assertTrue(deleteResult.cas() > 0);
        assertTrue(upsertResult.cas() != deleteResult.cas());
    }

    @Test
    public void testReturnPathInvalidOnDictAddForArrayPath() {
        String testSubKey = "testReturnPathInvalidOnDictAddForArrayPath";
        bucket.upsert(JsonDocument.create(testSubKey,
                JsonObject.create().put("sub",
                        JsonObject.create().put("array",
                                JsonObject.empty()))));

        String subPath = "sub.array[1]";
        ByteBuf fragment = Unpooled.copiedBuffer("\"insertedPath\"", CharsetUtil.UTF_8);
        ReferenceCountUtil.releaseLater(fragment);

        //mutate
        SubDictAddRequest insertRequest = new SubDictAddRequest(testSubKey, subPath, fragment, bucket.name());
        assertFalse(insertRequest.createIntermediaryPath());

        SimpleSubdocResponse insertResponse = bucket.core().<SimpleSubdocResponse>send(insertRequest).toBlocking().single();
        ReferenceCountUtil.releaseLater(insertResponse.content());
        assertFalse(insertResponse.status().isSuccess());
        assertEquals(0, insertResponse.content().readableBytes());
        assertEquals(ResponseStatus.SUBDOC_PATH_INVALID, insertResponse.status());
    }

    @Test
    public void testReturnDeltaRangeOnCounterDeltaUnderflow() {
        String testSubKey = "shouldReturnDeltaRangeOnCounterDeltaUnderflow";
        bucket.upsert(JsonDocument.create(testSubKey,
                JsonObject.create().put("counter", 0)));

        String path = "counter";

        //first request will bring the value to -1
        long prepareUnderflow = -1L;
        SubCounterRequest request = new SubCounterRequest(testSubKey, path, prepareUnderflow, bucket.name());
        SimpleSubdocResponse response = bucket.core().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        String result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals("-1", result);

        //second request will underflow
        long delta = Long.MIN_VALUE;
        request = new SubCounterRequest(testSubKey, path, delta, bucket.name());
        response = bucket.core().<SimpleSubdocResponse>send(request).toBlocking().single();
        ReferenceCountUtil.releaseLater(response.content());
        result = response.content().toString(CharsetUtil.UTF_8);

        assertEquals(result, 0, result.length());
        assertEquals(ResponseStatus.SUBDOC_DELTA_RANGE, response.status());
    }

    @Test(expected = CASMismatchException.class)
    public void testFailWithInvalidCASOnAppend() {
        StringDocument stored = bucket.upsert(StringDocument.create("appendCasMismatch", "foo"));
        bucket.append(StringDocument.from(stored, stored.cas() + 1));
    }

    @Test(expected = CASMismatchException.class)
    public void testFailWithInvalidCASOnPrepend() {
        StringDocument stored = bucket.upsert(StringDocument.create("prependCasMismatch", "foo"));
        bucket.prepend(StringDocument.from(stored, stored.cas() + 1));
    }

    @Test(expected = RequestTooBigException.class)
    public void testSubdocTooBigException() {
        String testSubKey = "tooBigBodyForSubdoc";
        char[] chars = new char[Info.itemSizeMax() - 15];
        Arrays.fill(chars, 'x');
        try {
            bucket.upsert(JsonDocument.create(testSubKey,
                    JsonObject.create().put("value",
                            JsonArray.create().add(new String(chars)))));
        } catch (RequestTooBigException ex) {
            throw new RuntimeException("not expected TooBig here", ex);
        }
        bucket.mutateIn(testSubKey)
                .arrayAppend("value", "123456789012345")
                .execute();
    }
}
