package org.couchbase.mock.http.capi;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* Created by mnunberg on 12/18/14.
*/
public final class PathInfo {
    static final Pattern DESIGN_PATTERN = Pattern.compile("_design/([^/]*)");
    static final Pattern VIEW_PATTERN = Pattern.compile("_view/(.*)$");
    private final String design;
    private final String view;
    public PathInfo(String s) {
        Matcher designMatcher = DESIGN_PATTERN.matcher(s);
        Matcher viewMatcher = VIEW_PATTERN.matcher(s);
        String tmpDesign = "";
        String tmpView = "";

        if (designMatcher.find()) {
            tmpDesign = designMatcher.group(1);
        }
        if (viewMatcher.find()) {
            tmpView = viewMatcher.group(1);
        }

        try {
            tmpDesign = URLDecoder.decode(tmpDesign, "UTF-8");
            tmpView = URLDecoder.decode(tmpView, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }

        design = tmpDesign;
        view = tmpView;
    }

    public String getDesignName() { return design; }
    public String getViewName() { return view; }
    public String getDesignId() {
        return String.format("_design/%s", design);
    }
}
