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

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.io.StringWriter;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of a SCRAM-SHA512, SCRAM-SHA256 and SCRAM-SHA1 enabled
 * {@link SaslServer}.
 * <p>
 * Freely inspired from <a href="https://github.com/trondn/java-sasl-scram-sha1/blob/master/src/com/couchbase/security/sasl/scram/ShaImpl.java">java-sasl-scram-sha1 by Trond Norbye</a>
 *
 * @author Senol Ozer / Amadeus IT Group
 */
public class ShaSaslServer implements SaslServer {

    private static final byte[] CLIENT_KEY = "Client Key".getBytes();
    private static final byte[] SERVER_KEY = "Server Key".getBytes();

    private final String name;
    private final String hmacAlgorithm;
    private final CallbackHandler callbacks;
    private final MessageDigest digest;
    private String username;
    private String clientNonce;
    private String serverNonce;
    private byte[] salt;
    private byte[] saltedPassword;
    private int iterationCount;
    private String clientFirstMessage;
    private String clientFirstMessageBare;
    private String clientFinalMessageNoProof;
    private String serverFirstMessage;

    public ShaSaslServer(CallbackHandler cbh, int sha) throws NoSuchAlgorithmException {
        callbacks = cbh;
        switch (sha) {
            case 512:
                digest = MessageDigest.getInstance("SHA-512");
                name = "SCRAM-SHA512";
                hmacAlgorithm = "HmacSHA512";
                break;
            case 256:
                digest = MessageDigest.getInstance("SHA-256");
                name = "SCRAM-SHA256";
                hmacAlgorithm = "HmacSHA256";
                break;
            case 1:
                digest = MessageDigest.getInstance("SHA-1");
                name = "SCRAM-SHA1";
                hmacAlgorithm = "HmacSHA1";
                break;
            default:
                throw new RuntimeException("Invalid SHA version specified");
        }

        byte[] randomNonce = new byte[21];
        SecureRandom random = new SecureRandom();
        random.nextBytes(randomNonce);
        serverNonce = new String(Base64.getEncoder().encode(randomNonce));
        iterationCount = 4096;
    }

    /**
     * XOR the two arrays and store the result in the first one.
     *
     * @param result Where to store the result
     * @param other  The other array to xor with
     */
    private static void xor(byte[] result, byte[] other) {
        for (int i = 0; i < result.length; ++i) {
            result[i] = (byte) (result[i] ^ other[i]);
        }
    }

    private static void decodeAttributes(HashMap<String, String> attributes, String string) {
        String[] tokens = string.split(",");
        for (String token : tokens) {
            int idx = token.indexOf('=');
            if (idx != 1) {
                throw new IllegalArgumentException("the input string is not according to the spec");
            }
            String key = token.substring(0, 1);
            if (attributes.containsKey(key)) {
                throw new IllegalArgumentException("The key " + key + " is specified multiple times");
            }
            attributes.put(key, token.substring(2));
        }
    }

    @Override
    public String getMechanismName() {
        return name;
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException {
        if (clientFirstMessage == null) {
            return evaluateClientFirstMessage(response);
        } else if (clientFinalMessageNoProof == null) {
            return evaluateClientFinalMessage(response);
        }

        throw new SaslException("Can't evaluate challenge on a session which is complete");
    }

    private byte[] evaluateClientFinalMessage(byte[] response) {
        String clientFinalMessage = new String(response);

        HashMap<String, String> attributes = new HashMap<String, String>();
        decodeAttributes(attributes, clientFinalMessage);

        if (!attributes.containsKey("p")) {
            throw new IllegalArgumentException("client-final-message does not contain client proof");
        }

        int idx = clientFinalMessage.indexOf(",p=");
        clientFinalMessageNoProof = clientFinalMessage.substring(0, idx);

        // Generate the server signature
        byte[] serverSignature = getServerSignature();

        StringWriter writer = new StringWriter();
        writer.append("v=");
        writer.append(new String(Base64.getEncoder().encode(serverSignature)));

        // validate the client proof to see if we're getting the same value...
        String myClientProof = new String(Base64.getEncoder().encode(getClientProof()));
        if (!myClientProof.equals(attributes.get("p"))) {
            writer.append(",e=failed");
        }

        return writer.toString().getBytes();
    }

    private byte[] evaluateClientFirstMessage(byte[] response) throws SaslException {
        // the "client-first-message" message should contain a gs2-header
        // gs2-bind-flag,[authzid],client-first-message-bare
        clientFirstMessage = new String(response);

        // according to the RFC the client should not send 'y' unless the
        // server advertised SCRAM-SHA[n]-PLUS (which we don't)
        if (!clientFirstMessage.startsWith("n,")) {
            // We don't support the p= to do channel bindings (that should
            // be advertised with SCRAM-SHA[n]-PLUS)
            throw new SaslException("Invalid gs2 header");
        }

        // next up is an optional authzid which we completely ignore...
        int idx = clientFirstMessage.indexOf(',', 2);
        if (idx == -1) {
            throw new SaslException("Invalid gs2 header");
        }

        clientFirstMessageBare = clientFirstMessage.substring(idx + 1);

        HashMap<String, String> attributes = new HashMap<String, String>();
        decodeAttributes(attributes, clientFirstMessageBare);

        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            switch (entry.getKey().charAt(0)) {
                case 'n':
                    username = entry.getValue();
                    break;
                case 'r':
                    clientNonce = entry.getValue();
                    break;
                default:
                    throw new IllegalArgumentException("Invalid key supplied in the clientFirstMessageBare");
            }
        }

        if (username.isEmpty() || clientNonce.isEmpty()) {
            throw new IllegalArgumentException("username and client nonce is mandatory in clientFirstMessageBare");
        }

        salt = Base64.getDecoder().decode("QSXCR+Q6sek8bf92");
        generateSaltedPassword();

        String nonce = clientNonce + serverNonce;

        // build up the server-first-message
        StringBuilder writer = new StringBuilder();
        writer.append("r=");
        writer.append(nonce);
        writer.append(",s=");
        writer.append(new String(Base64.getEncoder().encode(salt)));
        writer.append(",i=");
        writer.append(Integer.toString(iterationCount));

        serverFirstMessage = writer.toString();
        return serverFirstMessage.getBytes();
    }

    @Override
    public boolean isComplete() {
        return clientFinalMessageNoProof != null;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        return new byte[0];
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        return new byte[0];
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        return null;
    }

    @Override
    public void dispose() throws SaslException {
        // Do nothing
    }

    /**
     * Generate the HMAC with the given SHA algorithm
     */
    private byte[] hmac(byte[] key, byte[] data) {
        try {
            final Mac mac = Mac.getInstance(hmacAlgorithm);
            mac.init(new SecretKeySpec(key, mac.getAlgorithm()));
            return mac.doFinal(data);
        } catch (InvalidKeyException e) {
            if (key.length == 0) {
                throw new UnsupportedOperationException("This JVM does not support empty HMAC keys (empty passwords). "
                        + "Please set a bucket password or upgrade your JVM.");
            } else {
                throw new RuntimeException("Failed to generate HMAC hash for password", e);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Generate the hash of the function.. Unfortunately we couldn't use the one
     * provided by the Java framework because it didn't support others than SHA1.
     * See https://www.ietf.org/rfc/rfc5802.txt (page 6) for how it is generated.
     *
     * @param password   The password to use
     * @param salt       The salt used to salt the hash function
     * @param iterations The number of iterations to sue
     * @return The pbkdf2 version of the password
     */
    private byte[] pbkdf2(final String password, final byte[] salt, int iterations) {
        try {
            Mac mac = Mac.getInstance(hmacAlgorithm);
            Key key;
            if (password == null || password.isEmpty()) {
                key = new EmptySecretKey(hmacAlgorithm);
            } else {
                key = new SecretKeySpec(password.getBytes(), hmacAlgorithm);
            }
            mac.init(key);
            mac.update(salt);
            mac.update("\00\00\00\01".getBytes()); // Append INT(1)

            byte[] un = mac.doFinal();
            mac.update(un);
            byte[] uprev = mac.doFinal();
            xor(un, uprev);

            for (int i = 2; i < iterations; ++i) {
                mac.update(uprev);
                uprev = mac.doFinal();
                xor(un, uprev);
            }

            return un;
        } catch (InvalidKeyException e) {
            if (password == null || password.isEmpty()) {
                throw new UnsupportedOperationException("This JVM does not support empty HMAC keys (empty passwords). "
                        + "Please set a bucket password or upgrade your JVM.");
            } else {
                throw new RuntimeException("Failed to generate HMAC hash for password", e);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void generateSaltedPassword() throws SaslException {
        final PasswordCallback passwordCallback = new PasswordCallback("Password", false);
        try {
            callbacks.handle(new Callback[]{passwordCallback});
        } catch (IOException e) {
            throw new SaslException("Missing callback fetch password", e);
        } catch (UnsupportedCallbackException e) {
            throw new SaslException("Missing callback fetch password", e);
        }

        final char[] pw = passwordCallback.getPassword();
        if (pw == null) {
            throw new SaslException("Password can't be null");
        }

        String password = new String(pw);
        saltedPassword = pbkdf2(password, salt, iterationCount);
        passwordCallback.clearPassword();
    }

    /**
     * Generate the Server Signature. It is computed as:
     * <p/>
     * SaltedPassword := Hi(Normalize(password), salt, i) ServerKey :=
     * HMAC(SaltedPassword, "Server Key") ServerSignature := HMAC(ServerKey,
     * AuthMessage)
     */
    private byte[] getServerSignature() {
        byte[] serverKey = hmac(saltedPassword, SERVER_KEY);
        return hmac(serverKey, getAuthMessage().getBytes());
    }

    /**
     * Generate the Client Proof. It is computed as:
     * <p/>
     * SaltedPassword := Hi(Normalize(password), salt, i) ClientKey :=
     * HMAC(SaltedPassword, "Client Key") StoredKey := H(ClientKey) AuthMessage :=
     * client-first-message-bare + "," + server-first-message + "," +
     * client-final-message-without-proof ClientSignature := HMAC(StoredKey,
     * AuthMessage) ClientProof := ClientKey XOR ClientSignature
     */
    private byte[] getClientProof() {
        byte[] clientKey = hmac(saltedPassword, CLIENT_KEY);
        byte[] storedKey = digest.digest(clientKey);
        byte[] clientSignature = hmac(storedKey, getAuthMessage().getBytes());

        xor(clientKey, clientSignature);
        return clientKey;
    }

    /**
     * Get the AUTH message (as specified in the RFC)
     */
    private String getAuthMessage() {
        if (clientFirstMessageBare == null) {
            throw new RuntimeException("can't call getAuthMessage without clientFirstMessageBare is set");
        }
        if (serverFirstMessage == null) {
            throw new RuntimeException("can't call getAuthMessage without serverFirstMessage is set");
        }
        if (clientFinalMessageNoProof == null) {
            throw new RuntimeException("can't call getAuthMessage without clientFinalMessageNoProof is set");
        }
        return clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageNoProof;
    }

    /**
     * Not supported
     */
    @Override
    public String getAuthorizationID() {
        return null;
    }

    /**
     * SecretKeySpec doesn't support an empty password, god knows why. so lets just
     * fake it till they make it!
     */
    private static class EmptySecretKey implements SecretKey {
        private final String algorithm;

        public EmptySecretKey(String algorithm) {
            this.algorithm = algorithm;
        }

        @Override
        public String getAlgorithm() {
            return algorithm;
        }

        @Override
        public String getFormat() {
            return "RAW";
        }

        @Override
        public byte[] getEncoded() {
            return new byte[]{};
        }
    }
}
