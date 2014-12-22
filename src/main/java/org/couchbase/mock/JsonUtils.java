package org.couchbase.mock;


import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JsonUtils {
    final static public Gson GSON = new Gson();

    public static String encode(Object obj) {
        return GSON.toJson(obj);
    }
    public static <T> T decode(String json, Class<T> cls) {
        return GSON.fromJson(json, cls);
    }
    public static List decodeAsList(String json) {
        return decode(json, ArrayList.class);
    }
    public static Map<String,Object> decodeAsMap(String json) {
        return decode(json, HashMap.class);
    }
}
