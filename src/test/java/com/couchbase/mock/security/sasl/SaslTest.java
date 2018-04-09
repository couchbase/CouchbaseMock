/*
 * Copyright 2018 Couchbase, Inc.
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
package com.couchbase.mock.security.sasl;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.junit.Assert;
import org.junit.Test;
import com.couchbase.mock.memcached.SaslCallbackHandler;

/**
 * @author Senol Ozer / Amadeus IT Group
 */
public class SaslTest {


    /**
     * SunSASL provider doesn't support the server mechanism PLAIN
     * https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html#SUN
     * 
     * @throws SaslException
     */
    @Test(expected = SaslException.class)
    public void testSaslPlain() throws SaslException {
        Sasl.createSaslServer("PLAIN", "couchbase", "localhost", null, new SaslCallbackHandler("foo", "bar"));
    }

    @Test
    public void testSaslCramMd5() throws SaslException {
        SaslServer saslServer = Sasl.createSaslServer("CRAM-MD5", "couchbase", "localhost", null,
                new SaslCallbackHandler("foo", "bar"));
        Assert.assertEquals("CRAM-MD5", saslServer.getMechanismName());
        Assert.assertEquals(false, saslServer.isComplete());
        Assert.assertFalse(saslServer instanceof ShaSaslServer);
    }

    @Test
    public void testSaslScramSha512() throws SaslException {
        SaslServer saslServer = Sasl.createSaslServer("SCRAM-SHA512", "couchbase", "localhost", null,
                new SaslCallbackHandler("foo", "bar"));
        Assert.assertEquals("SCRAM-SHA512", saslServer.getMechanismName());
        Assert.assertEquals(false, saslServer.isComplete());
        Assert.assertTrue(saslServer instanceof ShaSaslServer);
    }

}
