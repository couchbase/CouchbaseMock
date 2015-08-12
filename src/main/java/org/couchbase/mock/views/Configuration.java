/**
 *     Copyright 2012 Couchbase, Inc.
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
package org.couchbase.mock.views;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.couchbase.mock.JsonUtils;
import org.mozilla.javascript.NativeObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Sergey Avseyev
 *
 * This class is used as the collection of view input parameters. The accessors are mainly here for tests.
 */
public class Configuration {
    final Map<String,String> optMap = new HashMap<String,String>();
    public static final String PARAM_ENDKEY = "endkey";
    public static final String PARAM_STARTKEY = "startkey";
    public static final String PARAM_ENDKEY_DOCID = "endkey_docid";
    public static final String PARAM_STARTKEY_DOCID = "startkey_docid";
    public static final String PARAM_LIMIT = "limit";
    public static final String PARAM_SKIP = "skip";
    public static final String PARAM_REDUCE = "reduce";
    public static final String PARAM_GROUP = "group";
    public static final String PARAM_GROUP_LEVEL = "group_level";
    public static final String PARAM_KEY_SINGLE = "key";
    public static final String PARAM_KEY_MULTI = "keys";
    public static final String PARAM_INCLUSIVE_END = "inclusive_end";
    public static final String PARAM_INCLUSIVE_START = "inclusive_start";
    public static final String PARAM_DESCENDING = "descending";

    public Configuration() {
    }

    public Configuration(Map<String,String> params) {
        optMap.putAll(params);
    }

    public void setJson(String key, int number) {
        optMap.put(key, Integer.toString(number));

    }
    public void setJson(String key, String value) {
        optMap.put(key, new JsonPrimitive(value).toString());
    }

    public void setJson(String key, boolean value) {
        optMap.put(key, value ? "true" : "false");
    }

    public void setRaw(String key, String value) {
        optMap.put(key, value);
    }

    public void setDescending(boolean descending) {
        setJson(PARAM_DESCENDING, descending);
    }

    public void setEndKey(String endkey) {
        setJson(PARAM_ENDKEY, endkey);
    }

    public void setStartKey(String startkey) {
        setJson(PARAM_STARTKEY, startkey);
    }

    public void setStartKey(int startKey) {
        setJson(PARAM_STARTKEY, startKey);
    }

    public void setEndKey(int endKey) {
        setJson(PARAM_ENDKEY, endKey);
    }

    public void setGroup(boolean group) {
        setJson(PARAM_GROUP, group);
    }

    public void setGroupLevel(int group_level) {
        setJson(PARAM_GROUP_LEVEL, group_level);
    }

    public void setInclusiveEnd(boolean inclusive_end) {
        setJson(PARAM_INCLUSIVE_END, inclusive_end);
    }

    public void setKey(String key) {
        setJson(PARAM_KEY_SINGLE, key);
    }

    public void setEncodedKey(String key) {
        setRaw(PARAM_KEY_SINGLE, key);
    }

    public void setLimit(int limit) {
        setJson(PARAM_LIMIT, limit);
    }

    public void setReduce(boolean reduce) {
        setJson(PARAM_REDUCE, reduce);
    }

    public void setSkip(int skip) {
        setJson(PARAM_SKIP, skip);
    }

    public void setEncodedKeys(List<String> keys) {
        JsonArray decKeys = new JsonArray();
        for (String encKey : keys) {
            JsonElement decKey = JsonUtils.GSON.fromJson(encKey, JsonElement.class);
            decKeys.add(decKey);
        }
        setRaw(PARAM_KEY_MULTI, decKeys.toString());
    }

    NativeObject toNativeObject() {
        NativeObject obj = new NativeObject();
        for (Map.Entry<String,String> ent : optMap.entrySet()) {
            obj.put(ent.getKey(), obj, ent.getValue());
        }
        return obj;
    }
}
