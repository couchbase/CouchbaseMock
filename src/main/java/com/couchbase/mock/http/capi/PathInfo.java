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

package com.couchbase.mock.http.capi;

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
