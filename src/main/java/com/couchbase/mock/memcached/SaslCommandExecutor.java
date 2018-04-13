/*
 * Copyright 2017 Couchbase, Inc.
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
package com.couchbase.mock.memcached;

import java.net.ProtocolException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.BinarySaslResponse;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.ErrorCode;
import com.couchbase.mock.security.sasl.Sasl;

/**
 * @author Sergey Avseyev
 */
public class SaslCommandExecutor implements CommandExecutor {
    // http://www.ietf.org/rfc/rfc4616.txt
    // http://www.ietf.org/rfc/rfc5802.txt

    private static final String PROTOCOL_COUCHBASE = "couchbase";

    private SaslServer saslServer;

    @Override
    public BinaryResponse execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client)
            throws ProtocolException {
        CommandCode cc = cmd.getComCode();

        BinaryResponse response;
        switch (cc) {
            case SASL_LIST_MECHS:
                response = new BinarySaslResponse(cmd, "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN");
                break;
            case SASL_AUTH:
                if (cmd.getKey() == null || "PLAIN".equals(cmd.getKey())) {
                    response = plainAuth(cmd, server, client);
                } else {
                    createSaslServer(cmd, server, client);
                    response = saslAuth(cmd, client);
                }
                break;
            case SASL_STEP:
                response = saslAuth(cmd, client);
                break;
            default:
                response = new BinarySaslResponse(cmd);
                break;
        }
        return response;

    }

    private BinarySaslResponse plainAuth(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        byte[] raw = cmd.getValue();
        String[] strs = new String[3];

        int offset = 0;
        int oix = 0;

        for (int ii = 0; ii < raw.length; ii++) {
            if (raw[ii] == 0x0) {
                strs[oix++] = new String(raw, offset, ii - offset);
                offset = ii + 1;
            }
        }
        strs[oix] = new String(raw, offset, raw.length - offset);

        String user = strs[1];
        String pass = strs[2];

        Bucket bucket = server.getBucket();
        if (!bucket.getName().equals(user)) {
            return new BinarySaslResponse(cmd);
        }

        String bPass = bucket.getPassword();
        if (bPass.equals(pass)) {
            client.setAuthenticated();
            return new BinarySaslResponse(cmd, "Authenticated");
        } else {
            return new BinarySaslResponse(cmd);
        }
    }

    private void createSaslServer(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client)
            throws ProtocolException {
        try {
            String serverName = client.getServer().getHostname();
            String mechanism = cmd.getKey();
            Bucket bucket = server.getBucket();
            String password = bucket.getPassword();
            String username = bucket.getName();
            saslServer = Sasl.createSaslServer(mechanism, PROTOCOL_COUCHBASE, serverName, null,
                    new SaslCallbackHandler(username, password));
        } catch (SaslException e) {
            throw new ProtocolException(e.getMessage());
        }
    }

    private BinaryResponse saslAuth(BinaryCommand cmd, MemcachedConnection client) throws ProtocolException {
        byte[] raw = cmd.getValue();
        try {
            final byte[] challenge = saslServer.evaluateResponse(raw);
            if (saslServer.isComplete()) {
                client.setAuthenticated();
                return new BinarySaslResponse(cmd, new String(challenge));
            } else {
                return new BinarySaslResponse(cmd, new String(challenge), ErrorCode.AUTH_CONTINUE);
            }
        } catch (SaslException e) {
            throw new ProtocolException(e.getMessage());
        }
    }
}
