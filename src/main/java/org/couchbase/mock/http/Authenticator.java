/**
 *     Copyright 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.couchbase.mock.http;

import com.sun.net.httpserver.BasicAuthenticator;

/**
 *
 * @author Sergey Avseyev
 */
public class Authenticator extends BasicAuthenticator {

    private String username;
    private String password;

    public Authenticator(String username, String password) {
        super("Couchbase Mock");
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean checkCredentials(String username, String password) {
        return this.username.equals(username) && this.password.equals(password);
    }
}
