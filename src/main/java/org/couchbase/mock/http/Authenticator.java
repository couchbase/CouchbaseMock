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

import org.couchbase.mock.Bucket;

/**
 * @author Sergey Avseyev
 */
public class Authenticator {

    private final String adminName;
    private final String adminPass;

    public Authenticator(String username, String password) {
        this.adminName = username;
        this.adminPass = password;
    }

    public boolean isAuthorizedForBucket(AuthContext ctx, Bucket bucket) {
        if (ctx.getUsername().equals(adminName)) {
            return ctx.getPassword().equals(adminPass);
        }

        if (bucket.getName().equals(ctx.getUsername())) {
            return bucket.getPassword().equals(ctx.getPassword());
        }

        return bucket.getPassword().isEmpty() && ctx.getPassword().isEmpty();
    }

    public boolean isAdministrator(AuthContext ctx) {
        return ctx.getUsername() != null && ctx.getUsername().equals(adminName) &&
                ctx.getPassword() != null && ctx.getPassword().equals(adminPass);
    }

    public String getAdminName() {
        return adminName;
    }

}
