package org.couchbase.mock.http.capi;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.JsonUtils;
import org.couchbase.mock.http.HttpAuthVerifier;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.httpio.HttpServer;
import org.couchbase.mock.views.DesignDocument;
import org.couchbase.mock.views.View;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class CAPIServer {
    private final Bucket bucket;
    final HttpAuthVerifier verifier;

    private final Map<String, DesignDocument> designDocMap;
    private HttpServer parentServer = null;

    public CAPIServer(Bucket bucket, HttpAuthVerifier verifier) {
        this.bucket = bucket;
        this.verifier = verifier;
        this.designDocMap = new ConcurrentHashMap<String, DesignDocument>();
    }

    public void register(HttpServer server) {
        DesignHandler designServer = new DesignHandler(this);
        String prefix = String.format("/%s/_design/*", bucket.getName());
        server.register(prefix, designServer);
        parentServer = server;
    }

    private String makeViewPaths(DesignDocument design, View view) {
        return String.format("/%s/%s/_view/%s", bucket.getName(), design.getId(), view.getName());
    }

    private void handleViewPaths(DesignDocument design, boolean enabled) {
        for (View view : design.getViews()) {
            String path = makeViewPaths(design, view);
            if (enabled) {
                System.err.printf("Registering name '%s'\n", path);
                parentServer.register(path, new ViewHandler(view, bucket));
            } else {
                parentServer.unregister(path);
            }
        }
    }

    private void removeDesign(DesignDocument design, boolean needSync) {
        DesignDocument oldDocument;
        if (needSync) {
            synchronized (designDocMap) {
                oldDocument = designDocMap.remove(design.getId());
            }
        } else {
            oldDocument = designDocMap.remove(design.getId());
        }
        if (oldDocument != null) {
            handleViewPaths(design, false);
        }
    }

    public void removeDesign(DesignDocument design) {
        removeDesign(design, true);
    }

    void addDesign(DesignDocument design) {
        synchronized (designDocMap) {
            removeDesign(design, false);
            handleViewPaths(design, true);
            designDocMap.put(design.getId(), design);
        }
    }

    DesignDocument findDesign(PathInfo info) {
        return designDocMap.get(info.getDesignId());
    }

    public static String makeError(String errStr, String reasonStr) {
        Map<String,String> mm = new HashMap<String, String>();
        mm.put("error", errStr);
        mm.put("reason", reasonStr);
        return JsonUtils.encode(mm) + "\n";
    }

    public static String makeError(String reasonStr) {
        return makeError("unknown_error", reasonStr);
    }

    public static void makeNotFoundError(HttpResponse response) {
        HandlerUtil.makeJsonResponse(response, TXT_NOTFOUND);
        response.setStatusCode(HttpStatus.SC_NOT_FOUND);
    }

    public static void makeNotFoundError(HttpResponse response, String detail) {
        String s = makeError("not_found", detail);
        s += "\n";
        HandlerUtil.makeJsonResponse(response, s);
        response.setStatusCode(HttpStatus.SC_NOT_FOUND);
    }

    final static Set<String> ALLOWED_DDOC_METHODS = new HashSet<String>();
    static  {
        ALLOWED_DDOC_METHODS.add("GET");
        ALLOWED_DDOC_METHODS.add("PUT");
        ALLOWED_DDOC_METHODS.add("HEAD");
        ALLOWED_DDOC_METHODS.add("DELETE");
    }

    static private final String TXT_NOTFOUND = "{\"error\":\"not_found\",\"reason\":\"missing\"}\n";

}