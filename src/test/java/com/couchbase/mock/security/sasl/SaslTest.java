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

import com.couchbase.mock.memcached.SaslCallbackHandler;
import org.junit.Assert;
import org.junit.Test;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

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
    @Test
    public void testSaslPlain() throws SaslException {
        Assert.assertNull(
                Sasl.createSaslServer(
                        "PLAIN",
                        "localhost",
                        null,
                        new SaslCallbackHandler("foo", "bar")
                )
        );
    }

    @Test
    public void testSaslCramMd5() throws SaslException {
        SaslServer saslServer = Sasl.createSaslServer("CRAM-MD5", "localhost", null,
                new SaslCallbackHandler("foo", "bar"));
        Assert.assertEquals("CRAM-MD5", saslServer.getMechanismName());
        Assert.assertEquals(false, saslServer.isComplete());
        Assert.assertFalse(saslServer instanceof ShaSaslServer);
    }

    @Test
    public void testSaslScramSha512() throws SaslException {
        SaslServer saslServer = Sasl.createSaslServer("SCRAM-SHA512", "localhost", null,
                new SaslCallbackHandler("foo", "bar"));
        Assert.assertEquals("SCRAM-SHA512", saslServer.getMechanismName());
        Assert.assertEquals(false, saslServer.isComplete());
        Assert.assertTrue(saslServer instanceof ShaSaslServer);
    }

    @Test
    public void testNonexistentMechanism() throws SaslException {
        Assert.assertNull(
                Sasl.createSaslServer(
                        "SCRAP-SHA512",
                        "localhost",
                        null,
                        new SaslCallbackHandler("foo", "bar")
                )
        );
    }
}
