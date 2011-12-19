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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.couchbase.mock.Bucket;

/**
 *
 * @author Sergey Avseyev
 */
public class Authenticator extends BasicAuthenticator {

    private String adminName;
    private String adminPass;
    private String bucketName;
    private Map<String, Bucket> buckets;

    public Authenticator(String username, String password, Map<String, Bucket> buckets) {
        super("Couchbase Mock");
        this.adminName = username;
        this.adminPass = password;
        this.buckets = buckets;
    }

    @Override
    public boolean checkCredentials(String username, String password) {
        if (adminName.equals(username) && adminPass.equals(password)) {
            return true;
        }

        Bucket bucket = buckets.get(username);
        if (bucket == null || !bucketName.equals(username)) {
            return false;
        }
        return bucket.getPassword().equals(password);
    }

    @Override
    public Result authenticate(HttpExchange exchange) {
        String requestPath = exchange.getRequestURI().getPath();
        Matcher m = Pattern.compile("/pools/\\w+/buckets/(\\w+)/?.*").matcher(requestPath);
        bucketName = null;
        if (m.find()) {
            bucketName = m.group(1);
        }

        if (!exchange.getRequestHeaders().containsKey("Authorization")) {
            Bucket bucket = buckets.get(bucketName);
            if (bucket == null || bucket.getPassword().isEmpty()) {
                return new Authenticator.Success(new HttpPrincipal("", realm));
            }
        }
        return super.authenticate(exchange);
    }

    public String getAdminName() {
        return adminName;
    }

}
