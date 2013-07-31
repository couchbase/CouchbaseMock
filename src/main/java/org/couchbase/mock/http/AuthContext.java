/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.http;
import java.io.IOException;

import org.couchbase.mock.util.Base64;

/**
 *
 * @author mnunberg
 */
public class AuthContext {
    private String username;
    private String password;

    public AuthContext (String hdr) throws IOException {
        String[] parts = hdr.split(" ");
        if (parts.length != 2) {
            throw new IOException("Invalid auth header");
        }

        if (!parts[0].equals("Basic")) {
            throw new IOException("Non-Basic auth not supported");
        }

        String b64 = parts[1];
        String decoded = Base64.decode(b64);
        parts = decoded.split(":", 2);

        if (parts.length == 2) {
            username = parts[0];
            password = parts[1];
        } else {
            throw new IOException("Don't know what to do with " + decoded);
        }
    }

    public AuthContext () {
        username = "";
        password = "";
    }

    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }
}
