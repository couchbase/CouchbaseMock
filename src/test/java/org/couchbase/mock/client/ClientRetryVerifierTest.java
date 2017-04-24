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

package org.couchbase.mock.client;

import org.couchbase.mock.memcached.client.CommandBuilder;
import org.couchbase.mock.memcached.client.Retryer;
import org.couchbase.mock.memcached.errormap.ErrorMap;
import org.couchbase.mock.memcached.errormap.ErrorMapEntry;
import org.couchbase.mock.memcached.errormap.RetrySpec;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Created by mnunberg on 4/20/17.
 */
public class ClientRetryVerifierTest extends ClientBaseTest {
    private int vb;
    final private String key = "key";

    protected void setUp() throws Exception {
        super.setUp();
        vb = findValidVbucket(0);
    }

    protected Retryer setupRetryer(ErrorCode opcode) throws Exception {
        // Set up the opfail request
        MockRequest mockCmd = new OpfailRequest(opcode, 100, 0);
        assertTrue(mockClient.request(mockCmd).isOk());

        // Start logging
        mockCmd = new StartRetryVerifyRequest(0, bucketConfiguration.getName());
        assertTrue(mockClient.request(mockCmd).isOk());

        byte[] cmd = new CommandBuilder(CommandCode.GET).key(key, (short)vb).build();
        ErrorMapEntry entry = ErrorMap.DEFAULT_ERRMAP.getErrorEntry(opcode);
        assertNotNull(entry);
        RetrySpec spec = entry.getRetrySpec();
        assertNotNull(spec);
        return new Retryer(getBinClient(), spec, cmd);
    }

    protected void runSuccess(ErrorCode errcode, Retryer r) throws Exception {
        r.run();
        MockRequest mockCmd = new CheckRetryVerifyRequest(0, bucketConfiguration.getName(), CommandCode.GET, errcode);
        MockResponse resp = mockClient.request(mockCmd);
        assertTrue(resp.getRawJson().toString(), resp.isOk());
    }

    protected void runError(ErrorCode errcode, Retryer r) throws Exception {
        r.runError();
        MockRequest mockCmd = new CheckRetryVerifyRequest(0, bucketConfiguration.getName(), CommandCode.GET, errcode);
        MockResponse resp = mockClient.request(mockCmd);
        assertFalse(resp.isOk());
    }

    public void testConstant() throws Exception {
        Retryer r = setupRetryer(ErrorCode.DUMMY_RETRY_CONSTANT);
        runSuccess(ErrorCode.DUMMY_RETRY_CONSTANT, r);
        runError(ErrorCode.DUMMY_RETRY_CONSTANT, r);
    }

    public void testLinear() throws Exception {
        Retryer r = setupRetryer(ErrorCode.DUMMY_RETRY_LINEAR);
        runSuccess(ErrorCode.DUMMY_RETRY_LINEAR, r);
        runError(ErrorCode.DUMMY_RETRY_LINEAR, r);
    }

    public void testExponential() throws Exception {
        Retryer r = setupRetryer(ErrorCode.DUMMY_RETRY_EXPONENTIAL);
        runSuccess(ErrorCode.DUMMY_RETRY_EXPONENTIAL, r);
        runError(ErrorCode.DUMMY_RETRY_EXPONENTIAL, r);
    }
}
