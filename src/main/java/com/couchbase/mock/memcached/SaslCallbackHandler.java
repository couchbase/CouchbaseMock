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
package com.couchbase.mock.memcached;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.net.ProtocolException;

/**
 * Represents a chain of {@code CallbackHandler}s. The SCRAM-SHA* server
 * mechanism uses the {@code NameCallback} and {@code PasswordCallback} to obtain the password
 * required to verify the SASL client's response.
 *
 * @author Senol Ozer / Amadeus IT Group
 */
public final class SaslCallbackHandler implements CallbackHandler {

    /**
     * The username to auth against.
     */
    private final String username;

    /**
     * The password of the user.
     */
    private final String password;

    public SaslCallbackHandler(String username, String password) {
        super();
        this.username = username;
        this.password = password;
    }

    /**
     * Callback handler needed for the {@link SaslServer} which supplies username
     * and password.
     *
     * @param callbacks the possible callbacks.
     * @throws IOException                  if something goes wrong during negotiation.
     * @throws UnsupportedCallbackException if something goes wrong during
     *                                      negotiation.
     */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(username);
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password.toCharArray());
            } else {
                throw new ProtocolException("SASLServer requested unsupported callback: " + callback);
            }
        }
    }

}
