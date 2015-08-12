package org.couchbase.mock.http.capi;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.couchbase.mock.JsonUtils;
import org.couchbase.mock.httpio.HandlerUtil;
import org.couchbase.mock.views.DesignDocument;
import org.couchbase.mock.views.DesignParseException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
* Created by mnunberg on 12/18/14.
*/
class DesignHandler implements HttpRequestHandler {
    private final CAPIServer capiServer;

    public DesignHandler(CAPIServer capiServer) {
        this.capiServer = capiServer;
    }

    private static void setXCouchbaseMeta(HttpResponse response, DesignDocument ddoc) {
        // Generate the text:
        Map<String,Object> tmp = new HashMap<String,Object>();

        // PYCBC tests for this
        tmp.put("id", ddoc.getId());
        tmp.put("type", "json");
        tmp.put("rev", -1);

        String s = JsonUtils.encode(tmp);
        response.addHeader("X-Couchbase-Meta", s);
    }

    static void setOkResponse(HttpResponse response, DesignDocument ddoc) {
        JsonObject obj = new JsonObject();
        obj.addProperty("ok", true);
        obj.addProperty("id", ddoc.getId());
        HandlerUtil.makeJsonResponse(response, obj.toString());
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
        if (!capiServer.verifier.verify(request, response, context)) {
            return;
        }
        String mName = request.getRequestLine().getMethod();

        if (!CAPIServer.ALLOWED_DDOC_METHODS.contains(mName)) {
            response.setStatusCode(HttpStatus.SC_METHOD_NOT_ALLOWED);
            return;
        }

        PathInfo info = new PathInfo(request.getRequestLine().getUri());

        DesignDocument ddoc = capiServer.findDesign(info);


        if (!info.getViewName().isEmpty()) {
            // If we got here, it's because the view does not exist
            if (ddoc == null) {
                CAPIServer.makeNotFoundError(response, String.format("Design document %s not found", info.getDesignId()));
            } else {
                CAPIServer.makeNotFoundError(response, String.format("View `%s` not defined in local design document `%s`",
                        info.getViewName(), info.getDesignId()));
            }
            return;
        }

        if (!mName.equals("PUT") && ddoc == null) {
            CAPIServer.makeNotFoundError(response);
            return;
        }

        if (mName.equals("GET")) {
            HandlerUtil.makeJsonResponse(response, ddoc.getBody());
            setXCouchbaseMeta(response, ddoc);

        } else if (mName.equals("HEAD")) {
            response.setStatusCode(HttpStatus.SC_NOT_IMPLEMENTED);

        } else if (mName.equals("DELETE")) {
            capiServer.removeDesign(ddoc);
            setOkResponse(response, ddoc);

        } else if (mName.equals("PUT")) {
            if (! (request instanceof HttpEntityEnclosingRequest)) {
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                HandlerUtil.makeStringResponse(response, CAPIServer.makeError("badarg"));
                return;
            }

            Header ctHeader = request.getLastHeader(HttpHeaders.CONTENT_TYPE);
            if (ctHeader == null || !ctHeader.getValue().equals(ContentType.APPLICATION_JSON.getMimeType())) {
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                HandlerUtil.makeStringResponse(response, CAPIServer.makeError("invalid_design_document", "Content is not json."));
                return;
            }

            HttpEntity entity = ((HttpEntityEnclosingRequest)request).getEntity();
            String txt = EntityUtils.toString(entity);

            try {
                ddoc = DesignDocument.create(txt, info.getDesignName());
                capiServer.addDesign(ddoc);
                response.setStatusCode(HttpStatus.SC_CREATED);
                setXCouchbaseMeta(response, ddoc);

                // Set the response info
                setOkResponse(response, ddoc);

            } catch (DesignParseException ex) {
                String msg = ex.getMessage();
                if (msg == null) {
                    msg = "Couldn't parse JSON";
                }
                HandlerUtil.make400Response(response, CAPIServer.makeError("bad_request", msg));

            } catch (Exception ex) {
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                HandlerUtil.make400Response(response, sw.toString());
            }
        }
    }
}
