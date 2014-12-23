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
import java.net.Socket;

public final class BucketHandlers {
    private static class StreamingHandler implements HttpRequestHandler {
        private final CouchbaseMock mock;
        private final HttpAuthVerifier verifier;
        private final Bucket bucket;

        StreamingHandler(Bucket bucket, CouchbaseMock mock, HttpAuthVerifier verifier) {
            this.mock = mock;
            this.verifier = verifier;
            this.bucket = bucket;
        }

        @Override
        public void handle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
            if (!verifier.verify(req, response, context)) {
                return;
            }

            HttpServerConnection htConn = HandlerUtil.getConnection(context);
            response.setHeader(HttpHeaders.TRANSFER_ENCODING, HTTP.CHUNK_CODING);
            response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
            response.setHeader(HttpHeaders.CONNECTION, HTTP.CONN_CLOSE);
            response.removeHeaders(HttpHeaders.CONTENT_LENGTH);

            // Write the response
            htConn.sendResponseHeader(response);
            htConn.flush();

            Socket s = HandlerUtil.getSocket(context);
            BucketsStreamingHandler streamingHandler = new BucketsStreamingHandler(mock.getMonitor(),bucket, s);

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
        final HttpAuthVerifier verifier = new HttpAuthVerifier(bucket, cbMock.getAuthenticator());
        final HttpRequestHandler staticHandler = new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
                if (!verifier.verify(req, response, context)) {
                    return;
                }
                HandlerUtil.makeJsonResponse(response, StateGrabber.getBucketJSON(bucket));
            }
        };

        final StreamingHandler streamingHandler = new StreamingHandler(bucket, cbMock, verifier);
        final HttpRequestHandler flushHandler = new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
                if (!verifier.verify(req, response, context)) {
                    return;
                }

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
}
