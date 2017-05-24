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

package com.couchbase.mock.http;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.protocol.HttpContext;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.httpio.HandlerUtil;

import java.io.IOException;

public class HttpAuthVerifier {
    final private Authenticator authenticator;
    final private Bucket bucket;
    public HttpAuthVerifier(Bucket bucket, Authenticator authenticator) {
        this.authenticator = authenticator;
        this.bucket = bucket;
    }
    public boolean verify(HttpRequest request, HttpResponse response, HttpContext context) throws IOException {
        // See if we need authentication
        AuthContext ctx = HandlerUtil.getAuth(context, request);
        if (!authenticator.isAuthorizedForBucket(ctx, bucket)) {
            response.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
            return false;
        }
        return true;
    }
}
