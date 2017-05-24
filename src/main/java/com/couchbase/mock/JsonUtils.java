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

package com.couchbase.mock;


import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JsonUtils {
    final static public Gson GSON = new Gson();

    /**
     * Encode a Java object as JSON
     * @param obj The object to encode
     * @return The encoded JSON string
     */
    public static String encode(Object obj) {
        return GSON.toJson(obj);
    }

    /**
     * Attempt to decode a JSON string as a Java object
     * @param json The JSON string to decode
     * @param cls The class to decode as
     * @param <T> Class parameter
     * @return An instance of {@code cls}, or an exception
     */
    public static <T> T decode(String json, Class<T> cls) {
        return GSON.fromJson(json, cls);
    }

    /**
     * Decode a JSON string as a Java list. The string must represent a JSON Array
     * @param json The JSON to decode
     * @return A list representing the JSON array
     */
    public static List decodeAsList(String json) {
        return decode(json, ArrayList.class);
    }

    /**
     * Decode a JSON string as Java map. The string must represent a JSON Object
     * @param json The JSON to decode
     * @return a map representing the JSON Object
     */
    @SuppressWarnings("unchecked")
    public static Map<String,Object> decodeAsMap(String json) {
        return decode(json, HashMap.class);
    }
}
