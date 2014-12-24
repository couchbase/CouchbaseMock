package org.couchbase.mock.http;

import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.JsonUtils;
import org.couchbase.mock.http.capi.CAPIServer;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.httpio.HttpServer;
import org.couchbase.mock.httpio.ResponseHandledException;
import org.couchbase.mock.memcached.MemcachedServer;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class BucketAdminServer {
    private final CouchbaseMock mock;
    private final HttpAuthVerifier verifier;
    private final Bucket bucket;
    private final HttpServer parentServer;

    private class StreamingHandler implements HttpRequestHandler {
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

    private class OneShotHandler implements HttpRequestHandler {
        public void handle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
            if (!verifier.verify(req, response, context)) {
                return;
            }
            String methodName = req.getRequestLine().getMethod();
            if (methodName.equals("GET")) {
                HandlerUtil.makeJsonResponse(response, StateGrabber.getBucketJSON(bucket));
            } else if (methodName.equals("DELETE")) {
                mock.getPoolsHandler().handleDeleteBucket(req, response, context, bucket);
            } else {
                response.setStatusCode(HttpStatus.SC_NOT_IMPLEMENTED);
            }
        }
    }

    private class FlushHandler implements HttpRequestHandler {
        @Override
        public void handle(HttpRequest req, HttpResponse response, HttpContext context) throws HttpException, IOException {
            if (!verifier.verify(req, response, context)) {
                return;
            }

            for (MemcachedServer server : bucket.getServers()) {
                server.flushAll();
            }
        }
    }

    private class DesignDocsHandler implements HttpRequestHandler {
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            if (!verifier.verify(request, response, context)) {
                return;
            }
            CAPIServer capi = bucket.getCAPIServer();
            if (capi == null) {
                HandlerUtil.makeStringResponse(response, "Not found");
                response.setStatusCode(HttpStatus.SC_NOT_FOUND);
                return;
            }

            Map<String,Object> ddInfo = capi.getDddocApiInfo();
            String encoded = JsonUtils.encode(ddInfo);
            HandlerUtil.makeJsonResponse(response, encoded);
        }
    }

    public BucketAdminServer(Bucket bucket, HttpServer server, CouchbaseMock mock) {
        this.bucket = bucket;
        this.mock = mock;
        this.parentServer = server;
        this.verifier = new HttpAuthVerifier(bucket, mock.getAuthenticator());
    }

    private static final String FMT_ONESHOT = "%s/buckets/%s";
    private static final String FMT_STREAM = "%s/bucketsStreaming/%s";
    private static final String FMT_DOFLUSH = "%s/buckets/%s/controller/doFlush";
    private static final String FMT_DDOCS = "%s/buckets/%s/ddocs";

    private String getPoolPrefix() {
        return String.format("/pools/%s", mock.getPoolName());
    }

    public void register() {
        String prefix = getPoolPrefix();
        parentServer.register(String.format(FMT_ONESHOT, prefix, bucket.getName()), new OneShotHandler());
        parentServer.register(String.format(FMT_STREAM, prefix, bucket.getName()), new StreamingHandler());
        parentServer.register(String.format(FMT_DOFLUSH, prefix, bucket.getName()), new FlushHandler());
        parentServer.register(String.format(FMT_DDOCS, prefix, bucket.getName()), new DesignDocsHandler());
    }

    public void shutdown() {
        String prefix = getPoolPrefix();
        for (String s : new String[] { FMT_ONESHOT, FMT_STREAM, FMT_DOFLUSH, FMT_DDOCS}) {
            String path = String.format(s, prefix, bucket.getName());
            parentServer.unregister(path);
        }
    }
}
