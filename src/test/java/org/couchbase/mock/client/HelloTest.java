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

import org.couchbase.mock.memcached.MemcachedConnection;
import org.couchbase.mock.memcached.client.ClientResponse;
import org.couchbase.mock.memcached.client.CommandBuilder;
import org.couchbase.mock.memcached.client.MemcachedClient;
import org.couchbase.mock.memcached.protocol.BinaryHelloCommand;

/** Tests that the basic HELLO functionality works. */
public class HelloTest extends ClientBaseTest {

    public void testHello() throws Exception {
        MemcachedClient binClient = getBinClient(0);
        ClientResponse resp;

        resp = binClient.sendRequest(CommandBuilder.buildHello("dummyClient",
                BinaryHelloCommand.Feature.MUTATION_SEQNO));
        assertTrue(resp.success());

        MemcachedConnection conn = binClient.getConnection(getServer(0));
        assertNotNull(conn);
        boolean[] features = conn.getSupportedFeatures();
        assertTrue(features[BinaryHelloCommand.Feature.MUTATION_SEQNO.getValue()]);

        // Try an actual mutation seqno command...
        short vbid = findValidVbucket(0);
        resp = binClient.sendRequest(CommandBuilder.buildStore("Hello", vbid, "World"));
        assertTrue(resp.success());
        assertEquals(16, resp.getExtras().length);

    }
}
