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

import java.security.NoSuchAlgorithmException;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.junit.Assert;
import org.junit.Test;
import com.couchbase.client.core.security.sasl.ShaSaslClientFactory;
import com.couchbase.mock.memcached.SaslCallbackHandler;

/**
 *
 * @author Senol Ozer / Amadeus IT Group
 *
 */
public class ShaSaslServerTest {

    @Test
    public void testEvaluateResponse() throws NoSuchAlgorithmException, SaslException {

        CallbackHandler callbackHandler = new SaslCallbackHandler("foo", "bar");

        SaslServer saslServer = new ShaSaslServerFactory().createSaslServer("SCRAM-SHA512", "couchbase", "localhost",
                null, callbackHandler);
        SaslClient saslClient = new ShaSaslClientFactory().createSaslClient(new String[] {"SCRAM-SHA512"}, null,
                "couchbase", "localhost", null, callbackHandler);

        // The client generates the client-first-message
        byte[] clientFirstMessage = saslClient.evaluateChallenge(new byte[] {});

        // The server generates the server-first-message
        byte[] serverFirstMessage = saslServer.evaluateResponse(clientFirstMessage);

        // The client generates the client-final-message
        byte[] clientFinalMessage = saslClient.evaluateChallenge(serverFirstMessage);

        // The server generates the server-final-message and concluding the
        // authentication exchange
        Assert.assertFalse(saslServer.isComplete());
        byte[] serverFinalMessage = saslServer.evaluateResponse(clientFinalMessage);
        Assert.assertTrue(saslServer.isComplete());

        // The client authenticates the server
        Assert.assertFalse(saslClient.isComplete());
        byte[] saslStepClient = saslClient.evaluateChallenge(serverFinalMessage);
        Assert.assertTrue(saslClient.isComplete());
        Assert.assertEquals(0, saslStepClient.length);
    }


    @Test(expected = SaslException.class)
    public void testEvaluateResponseWithWrongPassword() throws NoSuchAlgorithmException, SaslException {
        ShaSaslServer shaSaslSrever = new ShaSaslServer(new SaslCallbackHandler("foo", "foo"), 512);
        SaslClient shaSaslClient = new ShaSaslClientFactory().createSaslClient(new String[] {"SCRAM-SHA512"}, null,
                "couchbase", "localhost", null, new SaslCallbackHandler("foo", "bar"));

        byte[] firstEvaluateChallenge = shaSaslClient.evaluateChallenge(new byte[] {});
        byte[] saslAuth = shaSaslSrever.evaluateResponse(firstEvaluateChallenge);
        byte[] client = shaSaslClient.evaluateChallenge(saslAuth);
        byte[] saslStep = shaSaslSrever.evaluateResponse(client);

        shaSaslClient.evaluateChallenge(saslStep);
    }
}
