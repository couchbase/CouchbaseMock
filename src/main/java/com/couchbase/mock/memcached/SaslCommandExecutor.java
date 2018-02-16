/*
 * Copyright 2017 Couchbase, Inc.
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

import com.couchbase.mock.Bucket;
import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.BinarySaslResponse;
import com.couchbase.mock.memcached.protocol.CommandCode;

/**
 * @author Sergey Avseyev
 */
public class SaslCommandExecutor implements CommandExecutor {
    // http://www.ietf.org/rfc/rfc4616.txt

    @Override
    public BinaryResponse execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        CommandCode cc = cmd.getComCode();

        switch (cc) {
            case SASL_LIST_MECHS:
                return new BinarySaslResponse(cmd, "PLAIN");
            case SASL_AUTH:
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
            case SASL_STEP:
                // This is only useful when the above returns SASL_CONTINUE.  In this
                // implementation, only PLAIN is supported, so it never will
                return new BinarySaslResponse(cmd);
            default:
                return new BinarySaslResponse(cmd);
        }
    }
}
