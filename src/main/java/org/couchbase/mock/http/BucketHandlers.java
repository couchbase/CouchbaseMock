package org.couchbase.mock.http;

import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.httpio.HttpServer;
import org.couchbase.mock.httpio.ResponseHandledException;
import org.couchbase.mock.memcached.MemcachedServer;

import java.io.IOException;
import java.io.OutputStream;

public final class BucketHandlers {
    private static class StreamingHandler extends AuthRequiredHandler {
        private final CouchbaseMock mock;

        StreamingHandler(Bucket bucket, CouchbaseMock mock) {
            super(bucket, mock);
            this.mock = mock;
        }

        @Override
        protected void doHandle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
            HttpServerConnection htConn = HandlerUtil.getConnection(context);

            response.setHeader(HttpHeaders.TRANSFER_ENCODING, HTTP.CHUNK_CODING);
            response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
            response.setHeader(HttpHeaders.CONNECTION, HTTP.CONN_CLOSE);
            response.removeHeaders(HttpHeaders.CONTENT_LENGTH);

            // Write the response
            htConn.sendResponseHeader(response);
            htConn.flush();

            OutputStream os = HandlerUtil.getSocket(context).getOutputStream();
            BucketsStreamingHandler streamingHandler = new BucketsStreamingHandler(mock.getMonitor(),bucket, os);

            try {
                streamingHandler.startStreaming();
            } catch (InterruptedException ex) {
                throw new IOException(ex);
            }
            // Ensure it doesn't get processed
            throw new ResponseHandledException();
        }
    }

    public static void installBucketHandler(final Bucket bucket, final HttpServer server, final CouchbaseMock cbMock) {
        final AuthRequiredHandler staticHandler = new AuthRequiredHandler(bucket, cbMock) {
            @Override
            protected void doHandle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
                HandlerUtil.makeJsonResponse(response, StateGrabber.getBucketJSON(bucket));
            }
        };

        final AuthRequiredHandler streamingHandler = new StreamingHandler(bucket, cbMock);

        final AuthRequiredHandler flushHandler = new AuthRequiredHandler(bucket, cbMock) {
            @Override
            protected void doHandle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
                for (MemcachedServer server : bucket.getServers()) {
                    server.flushAll();
                }
            }
        };

        final String prefix = "/pools/"+cbMock.getPoolName();
        server.register(String.format("%s/buckets/%s", prefix, bucket.getName()), staticHandler);
        server.register(String.format("%s/bucketsStreaming/%s", prefix, bucket.getName()), streamingHandler);
        server.register(String.format("%s/buckets/%s/controller/doFlush", prefix, bucket.getName()), flushHandler);
    }

    abstract static class AuthRequiredHandler implements HttpRequestHandler {
        protected final String username;
        protected final Bucket bucket;
        protected final Authenticator authInfo;

        AuthRequiredHandler(Bucket bucket, CouchbaseMock mock) {
            this.username = bucket.getName();
            this.bucket = bucket;
            this.authInfo = mock.getAuthenticator();
        }

        protected abstract void doHandle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException;

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            // See if we need authentication
            AuthContext ctx = HandlerUtil.getAuth(context, request);
            if (!authInfo.isAuthorizedForBucket(ctx, bucket)) {
                response.setStatusCode(HttpStatus.SC_UNAUTHORIZED);
                return;
            }
            // Otherwise, simply invoke
            doHandle(request, response, context);
        }
    }
}
