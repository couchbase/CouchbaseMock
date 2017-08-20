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
import net.spy.memcached.internal.OperationFuture;

import java.nio.ByteBuffer;

public class ClientMiscTest extends ClientBaseTest {
    public void testUnknownOpcode() throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(24);
        bb.put((byte) 0x80); // Magic
        bb.put((byte) 0xff); // "Opcode"
        bb.putShort((short) 0); // keylen
        bb.putShort((short) 0); // extlen
        bb.putShort((short) 0); // vbucket
        bb.putInt(0); // bodylen
        bb.putInt(42); // opaque
        bb.putLong(0); // CAS..

        byte[] req = bb.array();
        ClientResponse resp = getBinClient().sendRequest(req);
        assertEquals(CommandCode.ILLEGAL, resp.getComCode());
        assertEquals((byte) 0xff, resp.getOpcode());
        assertEquals(42, resp.getOpaque());
        assertEquals(ErrorCode.UNKNOWN_COMMAND, resp.getStatus());

    }


    public void testGetRandomEmpty() throws Exception {
        ClientResponse resp;
        for (int i = 0; i < bucketConfiguration.numNodes; i++) {
            MemcachedClient binClient = getBinClient(i);

            // Ensure it's flushed
            resp = binClient.sendRequest((new CommandBuilder(CommandCode.FLUSH).build()));
            assertTrue(resp.success());

            byte[] req = (new CommandBuilder(CommandCode.GET_RANDOM).build());
            resp = binClient.sendRequest(req);
            assertFalse(resp.success());
            assertEquals(ErrorCode.KEY_ENOENT, resp.getStatus());
        }
    }

    public void testGetRandom() throws Exception {
        ClientResponse resp;
        CommandBuilder cb = new CommandBuilder(CommandCode.GET_RANDOM);
        String value = "value";
        for (int i = 0; i < bucketConfiguration.numNodes; i++) {
            // Ensure it's flushed
            MemcachedClient binClient = getBinClient(i);
            resp = binClient.sendRequest((new CommandBuilder(CommandCode.FLUSH).build()));
            assertTrue(resp.success());

            String key = getValidKeyFor(i);
            OperationFuture ft = client.set(key, value);
            ft.get();
            assertTrue(ft.getStatus().isSuccess());

            resp = binClient.sendRequest(cb.build());
            assertTrue(resp.success());
            assertEquals(key, resp.getKey());
            assertEquals(value, resp.getValue());
            assertEquals(ft.getCas(), new Long(resp.getCas()));
        }
    }

    public void testSelectBucket() throws Exception {
        CommandBuilder cb = new CommandBuilder(CommandCode.SELECT_BUCKET);
        cb.key(bucketConfiguration.getName(), (short) 0);
        ClientResponse resp = getBinClient().sendRequest(cb);
        assertTrue(resp.success());

        cb.key("non-exist-bucket", (short) 0);
        resp = getBinClient().sendRequest(cb);
        assertEquals(ErrorCode.EACCESS, resp.getStatus());
    }
}
