/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * The {@link SaslServerFactory} supporting SCRAM-SHA512, SCRAM-SHA256 and
 * SCRAM-SHA1 authentication methods.
 *
 * @author Trond Norbye
 * @version 1.2.5
 */
public class ShaSaslServerFactory implements SaslServerFactory {

    private static final String SCRAM_SHA512 = "SCRAM-SHA512";
    private static final String SCRAM_SHA256 = "SCRAM-SHA256";
    private static final String SCRAM_SHA1 = "SCRAM-SHA1";
    private static final String[] SUPPORTED_MECHS = {SCRAM_SHA512, SCRAM_SHA256, SCRAM_SHA1};

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props,
            CallbackHandler cbh) throws SaslException {

        int sha = 0;

        if (mechanism.equals(SCRAM_SHA512)) {
            sha = 512;
        } else if (mechanism.equals(SCRAM_SHA256)) {
            sha = 256;
        } else if (mechanism.equals(SCRAM_SHA1)) {
            sha = 1;
        } else {
            return null;
        }

        if (cbh == null) {
            throw new SaslException("Callback handler to get username/password required");
        }

        try {
            return new ShaSaslServer(cbh, sha);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
        return SUPPORTED_MECHS;
    }
}
