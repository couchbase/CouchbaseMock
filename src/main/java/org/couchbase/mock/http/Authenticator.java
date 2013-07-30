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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import java.io.IOException;
import java.util.Map;

import org.couchbase.mock.Bucket;

/**
 * @author Sergey Avseyev
 */
public class Authenticator extends BasicAuthenticator {

    private final String adminName;
    private final String adminPass;
    private String currentBucketName;
    private final Map<String, Bucket> buckets;

    public Authenticator(String username, String password, Map<String, Bucket> buckets) {
        super("Couchbase Mock");
        this.adminName = username;
        this.adminPass = password;
        this.buckets = buckets;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean checkCredentials(String username, String password) {
        // Always allow admin
        if (adminName.equals(username) && adminPass.equals(password)) {
            return true;
        }

        Bucket bucket = buckets.get(username);
        if (bucket == null) {
            // No such bucket
            return false;
        }

        // No credentials
        if (username.isEmpty()) {
            return bucket.getPassword().isEmpty();
        }

        if (!currentBucketName.equals(username)) {
            return false;
        }

        String bucketPassword = bucket.getPassword();
        return bucketPassword.isEmpty() || bucketPassword.equals(password);
    }

    @Override
    public Result authenticate(HttpExchange exchange) {
        String path = exchange.getRequestURI().normalize().getPath();
        path = path.replaceAll("^/", "");
        String[] components = path.split("/");
        exchange.setAttribute(realm, this);
        // pools, default, buckets(Streaming)/ bucket
        currentBucketName = null;

        if (components.length > 3 && components[2].startsWith("buckets")) {
            currentBucketName = components[3];
        }

        AuthContext context = null;
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        if (auth != null) {
            try {
                context = new AuthContext(auth);
            } catch (IOException e) {
                return new Failure(400);
            }
        }

        // For now, if we don't have a bucket name, just continue
        if (currentBucketName == null) {
            return new Authenticator.Success(new HttpPrincipal("", realm));
        }

        Bucket bucket = buckets.get(currentBucketName);
        if (bucket != null) {
            // Have a bucket
            if ((bucket.getPassword() == null || bucket.getPassword().isEmpty()) && context == null) {
                return new Authenticator.Success(new HttpPrincipal("", realm));
            }
        }

        return super.authenticate(exchange);
    }

    public String getAdminName() {
        return adminName;
    }

}
