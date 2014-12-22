package org.couchbase.mock.http;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.protocol.HttpContext;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.httpio.HandlerUtil;

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
