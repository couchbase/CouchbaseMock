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
 * This object is intended to be used as a global throughout the cluster. It primarily contains the administrative
 * credentials.
 *
 * @author Mark Nunberg
 * @author Sergey Avseyev
 */
public class Authenticator {

    private final String adminName;
    private final String adminPass;

    /**
     * @param username The <i>administrative</i> username (typically {@code Administrator})
     * @param password The <i>administrative</i> password (typically {@code Password})
     */
    public Authenticator(String username, String password) {
        this.adminName = username;
        this.adminPass = password;
    }

    /**
     * Determine if the given credentials allow access to the bucket
     * @param ctx The credentials
     * @param bucket The bucket to check against
     * @return true if the credentials match the bucket's credentials, or if the bucket is not password protected, or if
     * the credentials match the administrative credentials
     */
    public boolean isAuthorizedForBucket(AuthContext ctx, Bucket bucket) {
        if (ctx.getUsername().equals(adminName)) {
            return ctx.getPassword().equals(adminPass);
        }

        if (bucket.getName().equals(ctx.getUsername())) {
            return bucket.getPassword().equals(ctx.getPassword());
        }

        return bucket.getPassword().isEmpty() && ctx.getPassword().isEmpty();
    }

    /**
     * Check if the given credentials allow administrative access
     * @param ctx The credentials to check
     * @return true if the credentials match the administrative credentials
     */
    public boolean isAdministrator(AuthContext ctx) {
        return ctx.getUsername() != null && ctx.getUsername().equals(adminName) &&
                ctx.getPassword() != null && ctx.getPassword().equals(adminPass);
    }

    /** Get the administrative username */
    public String getAdminName() {
        return adminName;
    }
    /** Get the administrative password */
    public String getAdminPass() {
        return adminPass;
    }
}
