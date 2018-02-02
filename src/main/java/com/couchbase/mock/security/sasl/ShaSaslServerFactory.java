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

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * The {@link SaslServerFactory} supporting SCRAM-SHA512, SCRAM-SHA256 and
 * SCRAM-SHA1 authentication methods.
 *
 * @author Senol Ozer / Amadeus IT Group
 */
public class ShaSaslServerFactory implements SaslServerFactory {

    private static final String SCRAM_SHA512 = "SCRAM-SHA512";
    private static final String SCRAM_SHA256 = "SCRAM-SHA256";
    private static final String SCRAM_SHA1 = "SCRAM-SHA1";
    public static final String[] SUPPORTED_MECHS = {SCRAM_SHA1, SCRAM_SHA256, SCRAM_SHA512};

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props,
                                       CallbackHandler cbh) throws SaslException {

        int sha = getDigestSize(mechanism);

        if (sha == 0) {
            throw new SaslException("This SCRAM-SHA mechanism is not supported " + mechanism);
        }

        if (cbh == null) {
            throw new SaslException("Callback handler to get username/password required");
        }

        try {
            return new ShaSaslServer(cbh, sha);
        } catch (NoSuchAlgorithmException e) {
            throw new SaslException(e.getMessage(), e);
        }
    }

    private int getDigestSize(String mechanism) {
        int sha = 0;

        if (mechanism.equals(SCRAM_SHA512)) {
            sha = 512;
        } else if (mechanism.equals(SCRAM_SHA256)) {
            sha = 256;
        } else if (mechanism.equals(SCRAM_SHA1)) {
            sha = 1;
        }

        return sha;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
        return SUPPORTED_MECHS;
    }
}
