/*
 * Copyright 2016 Couchbase, Inc.
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
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.nio.ByteBuffer;

public class ClientMiscTest extends ClientBaseTest {
    public void testUnknownOpcode() throws Exception {
        ByteBuffer bb = ByteBuffer.allocate(24);
        bb.put((byte)0x80); // Magic
        bb.put((byte)0xff); // "Opcode"
        bb.putShort((short)0); // keylen
        bb.putShort((short) 0); // extlen
        bb.putShort((short)0); // vbucket
        bb.putInt(0); // bodylen
        bb.putInt(42); // opaque
        bb.putLong(0); // CAS..

        byte[] req = bb.array();
        ClientResponse resp = getBinClient().sendRequest(req);
        assertEquals(CommandCode.ILLEGAL, resp.getComCode());
        assertEquals((byte)0xff, resp.getOpcode());
        assertEquals(42, resp.getOpaque());
        assertEquals(ErrorCode.UNKNOWN_COMMAND, resp.getStatus());

    }
}
