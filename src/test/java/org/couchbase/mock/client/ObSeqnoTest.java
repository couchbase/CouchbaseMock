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

import org.couchbase.mock.memcached.client.ClientResponse;
import org.couchbase.mock.memcached.client.CommandBuilder;
import org.couchbase.mock.memcached.client.MemcachedClient;
import org.couchbase.mock.memcached.protocol.BinaryHelloCommand;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.nio.ByteBuffer;

/**
 * Created by mnunberg on 2/6/15.
 */
public class ObSeqnoTest extends ClientBaseTest {

    protected MemcachedClient binClient;
    protected short vbid;

    protected void setUp() throws Exception {
        super.setUp();
        binClient = getBinClient(0);
        vbid = findValidVbucket(0);
        ClientResponse resp;
        resp = binClient.sendRequest(CommandBuilder.buildHello("dummy", BinaryHelloCommand.Feature.MUTATION_SEQNO));
        assertTrue(resp.success());
    }

    public void testBasicResponse() throws Exception {
        ClientResponse resp;

        resp = binClient.sendRequest(CommandBuilder.buildHello("dummy", BinaryHelloCommand.Feature.MUTATION_SEQNO));
        assertTrue(resp.success());

        // Store an item
        resp = binClient.sendRequest(CommandBuilder.buildStore("key", vbid, "value"));
        assertTrue(resp.success());

        ByteBuffer bb = ByteBuffer.wrap(resp.getExtras());
        long uuid = bb.getLong();
        long seqno = bb.getLong();

        // Now, OBSERVE_SEQNO
        CommandBuilder cBuilder = new CommandBuilder(CommandCode.OBSERVE_SEQNO).
                vBucket(vbid)
                .value(ByteBuffer.allocate(8).putLong(uuid).array());
        // Assume it's OK
        resp = binClient.sendRequest(cBuilder);
        assertTrue(resp.success());

        // Read the format, uuid, and sequence number
        bb = resp.getRawValue();
        bb.rewind();
        assertEquals(0x00, bb.get()); // Format
        assertEquals(vbid, bb.getShort()); // VBucket
        assertEquals(uuid, bb.getLong()); // UUID
        assertEquals(seqno, bb.getLong()); // Persisted Seqno
        assertEquals(seqno, bb.getLong()); // Cached Seqno
    }

    public void testFailoverResponse() throws Exception {
        ClientResponse resp = binClient.sendRequest(CommandBuilder.buildStore("key2", vbid, "value"));
        assertTrue(resp.success());

        ByteBuffer bb = ByteBuffer.wrap(resp.getExtras());
        long stored_uuid = bb.getLong();
        long stored_seqno = bb.getLong();


        mockClient.request(new RegenVBCoordsRequest());
        CommandBuilder cBuilder = new CommandBuilder(CommandCode.OBSERVE_SEQNO)
                .vBucket(vbid)
                .value(ByteBuffer.allocate(8).putLong(stored_uuid).array());
        resp = binClient.sendRequest(cBuilder);
        assertTrue(resp.success());
        bb = resp.getRawValue();
        bb.rewind();

        assertEquals(0x01, bb.get()); // Failover format
        assertEquals(vbid, bb.getShort()); // The vBucket

        assertNotSame(stored_uuid, bb.getLong()); // New UUID
        bb.getLong(); // Persisted seq
        bb.getLong(); // Cached Seq

        assertEquals(stored_uuid, bb.getLong());
        assertEquals(stored_seqno, bb.getLong());
    }

    public void testBadUuid() throws Exception {
        ClientResponse resp = binClient.sendRequest(CommandBuilder.buildStore("key3", vbid, "value"));
        assertTrue(resp.success());
        ByteBuffer bb = ByteBuffer.wrap(resp.getExtras());
        long uuid = bb.getLong() + 1;

        CommandBuilder cBuilder = new CommandBuilder(CommandCode.OBSERVE_SEQNO)
                .vBucket(vbid)
                .value(ByteBuffer.allocate(8).putLong(uuid).array());

        resp = binClient.sendRequest(cBuilder);
        assertFalse(resp.success());
        assertEquals(ErrorCode.EINTERNAL, resp.getStatus());
    }
}
