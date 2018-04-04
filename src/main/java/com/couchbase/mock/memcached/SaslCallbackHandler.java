package com.couchbase.mock.memcached;

import java.io.IOException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslServer;
import com.couchbase.client.core.endpoint.kv.AuthenticationException;

/**
 * Represents a chain of {@code CallbackHandler}s. 
 * @author sozer
 *
 */
public class SaslCallbackHandler implements CallbackHandler {

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
     * @throws IOException if something goes wrong during negotiation.
     * @throws UnsupportedCallbackException if something goes wrong during
     *         negotiation.
     */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(username);
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password.toCharArray());
            } else {
                throw new AuthenticationException("SASLServer requested unsupported callback: " + callback);
            }
        }
    }

}
