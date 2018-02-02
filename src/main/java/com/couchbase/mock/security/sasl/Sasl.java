/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.couchbase.mock.security.sasl;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.Arrays;
import java.util.Map;

/**
 * A wrapper for {@link javax.security.sasl.Sasl} which first tries the
 * mechanisms that are supported out of the box and then tries our extended
 * range.
 *
 * @author Senol Ozer / Amadeus IT Group
 */
public class Sasl {
    private static final String PROTOCOL_COUCHBASE = "couchbase";


    /**
     * Custom server factory which supports the additional mechanisms.
     */
    private static ShaSaslServerFactory saslServerFactory = new ShaSaslServerFactory();

    /**
     * Creates a new {@link SaslServer} and first tries the JVM built-in servers
     * before falling back to {@link ShaSaslServer} implementations. The mechanisms are tried
     * in the order they arrive.
     *
     * @param mechanism  The non-null mechanism name. It must be an IANA-registered
     *                   name of a SASL mechanism. (e.g. "SCRAM-SHA512", "PLAIN").
     * @param serverName The fully qualified host name of the server, or null if the
     *                   server is not bound to any specific host name. If the mechanism does
     *                   not allow an unbound server, a SaslException will be thrown.
     * @param props      The possibly null set of properties used to select the SASL
     *                   mechanism and to configure the authentication exchange of the selected
     *                   mechanism. For example, if props contains the Sasl.POLICY_NOPLAINTEXT
     *                   property with the value "true", then the selected SASL mechanism must
     *                   not be susceptible to simple plain passive attacks. In addition to the
     *                   standard properties declared in this class, other, possibly
     *                   mechanism-specific, properties can be included. Properties not
     *                   relevant to the selected mechanism are ignored, including any map
     *                   entries with non-String keys.
     * @param cbh        The possibly null callback handler to used by the SASL mechanisms
     *                   to get further information from the application/library to complete
     *                   the authentication. For example, a SASL mechanism might require the
     *                   authentication ID, password and realm from the caller. The
     *                   authentication ID is requested by using a NameCallback. The password
     *                   is requested by using a PasswordCallback. The realm is requested by
     *                   using a RealmChoiceCallback if there is a list of realms to choose
     *                   from, and by using a RealmCallback if the realm must be entered.
     * @return A possibly null SaslServer created using the parameters supplied. If
     * null, cannot find a SaslServerFactory that will produce one.
     * @throws SaslException If cannot create a SaslServer because of an error.
     */
    public static SaslServer createSaslServer(String mechanism, String serverName, Map<String, ?> props, CallbackHandler cbh) throws SaslException {
        if (Arrays.binarySearch(ShaSaslServerFactory.SUPPORTED_MECHS, mechanism) < 0) {
            return javax.security.sasl.Sasl.createSaslServer(mechanism, PROTOCOL_COUCHBASE, serverName, props, cbh);
        } else {
            return saslServerFactory.createSaslServer(mechanism, PROTOCOL_COUCHBASE, serverName, props, cbh);
        }
    }
}
