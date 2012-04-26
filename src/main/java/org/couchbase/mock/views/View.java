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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import net.sf.json.util.JSONUtils;
import org.couchbase.mock.memcached.DataStore;
import sun.org.mozilla.javascript.NativeArray;
import sun.org.mozilla.javascript.Scriptable;
import sun.org.mozilla.javascript.ScriptableObject;

/**
 *
 * @author Sergey Avseyev
 */
public class View {

    public class CompilationError extends Exception {
    }

    public static class RowComparator implements Comparator<HashMap> {

        private Configuration config;

        public RowComparator(Configuration config) {
            this.config = config;
        }

        @Override
        public int compare(HashMap o1, HashMap o2) {
            Object key1 = View.parseJSON(o1.get("key"));
            Object key2 = View.parseJSON(o2.get("key"));
            Object id1 = View.parseJSON(o1.get("id"));
            Object id2 = View.parseJSON(o2.get("id"));
            int ret;

            if (config.isDescending()) {
                ret = jsonCompareTo(key2, key1);
                if (ret == 0) {
                    ret = jsonCompareTo(id2, id1);
                }
            } else {
                ret = jsonCompareTo(key1, key2);
                if (ret == 0) {
                    ret = jsonCompareTo(id1, id2);
                }
            }
            return ret;
        }

        public static int jsonCompareTo(Object a, Object b) {
            int result;

            if (a instanceof JSONObject) {
                if (b instanceof JSONObject) {
                    JSONObject ao = (JSONObject) a;
                    JSONObject bo = (JSONObject) b;
                    Set as = ao.keySet();
                    Iterator ai = as.iterator();

                    while (true) {
                        if (!ai.hasNext()) {
                            if (bo.isEmpty()) {
                                return 0;
                            }
                            return -1;
                        }
                        if (bo.isEmpty()) {
                            return 1;
                        }
                        Object key = ai.next();
                        if (!bo.containsKey(key)) {
                            return 1;
                        }
                        result = jsonCompareTo(ao.get(key), bo.get(key));
                        bo.remove(key);
                        if (result != 0) {
                            return result;
                        }
                    }
                } else {
                    return -1;
                }
            }
            if (b instanceof JSONObject) {
                return 1;
            }
            if (a instanceof JSONArray) {
                if (b instanceof JSONArray) {
                    Iterator ai = ( (JSONArray) a ).iterator();
                    Iterator bi = ( (JSONArray) b ).iterator();

                    while (true) {
                        if (!ai.hasNext()) {
                            if (!bi.hasNext()) {
                                return 0;
                            }
                            return -1;
                        }
                        if (!bi.hasNext()) {
                            return 1;
                        }
                        result = jsonCompareTo(ai.next(), bi.next());
                        if (result != 0) {
                            return result;
                        }
                    }
                } else {
                    return -1;
                }
            }
            if (b instanceof JSONArray) {
                return 1;
            }
            if (JSONUtils.isNumber(a) && JSONUtils.isNumber(b)) {
                return (int) Math.floor(( (Number) a ).doubleValue() - ( (Number) b ).doubleValue());
            }
            /* in other cases values should be comparable */
            Comparable ac = (Comparable) a;
            Comparable bc = (Comparable) b;
            return ac.compareTo(bc);
        }
    }
    public static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz");
    private final String name;
    private final String mapSource;
    private final String reduceSource;
    private final ScriptEngine jsEngine;
    private final Mapper mapper;
    private Reducer reducer;

    public View(String name, String map) throws ScriptException {
        this(name, map, null);
    }

    public View(String name, String map, String reduce) throws ScriptException {
        this.name = name;
        this.mapSource = map;
        this.reduceSource = reduce;
        jsEngine = new ScriptEngineManager().getEngineByName("javascript");
        jsEngine.eval("emit = function(key, value) { result.push([key, value]) }");
        jsEngine.eval("sum = function(values) { var sum = 0; for (var i = 0; i < values.length; ++i) { sum += values[i]; }; return sum; }");
        this.mapper = new Mapper(jsEngine, this.mapSource);
        if (this.reduceSource != null) {
            this.reducer = new Reducer(jsEngine, this.reduceSource);
        }
    }

    public String getName() {
        return name;
    }

    public String getMapSource() {
        return mapSource;
    }

    public String getReduceSource() {
        return reduceSource;
    }

    public HashMap execute(DataStore store) {
        return execute(store, null);
    }

    public HashMap execute(DataStore store, Configuration config) {
        ArrayList<HashMap> rows;

        if (config == null) {
            config = new Configuration();
        }

        rows = mapper.execute(store, config);
        if (config.reduce() && reducer != null) {
            try {
                rows = reducer.execute(rows, config);
            } catch (ScriptException ex) {
                Logger.getLogger(View.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        Collections.sort(rows, new RowComparator(config));

        int totalRows = rows.size();
        int offset = 0;
        int end = rows.size();

        /* ranging: start_key, end_key, start_key_docid, end_key_docid */
        if (config.hasRange()) {
            Object startKey = parseJSON(config.getStartKey());
            Object startKeyDocId = parseJSON(config.getStartKeyDocId());
            Object endKey = parseJSON(config.getEndKey());
            Object endKeyDocId = parseJSON(config.getEndKeyDocId());
            if (config.isDescending()) {
                Object tmp;
                tmp = startKey;
                startKey = endKey;
                endKey = tmp;
                tmp = startKeyDocId;
                startKeyDocId = endKeyDocId;
                endKeyDocId = tmp;
            }
            for (int i = 0; i < rows.size(); ++i) {
                HashMap row = rows.get(i);
                Object key = row.get("key");
                Object id = row.get("id");
                if (key.equals(startKey) && offset == 0
                        && ( startKeyDocId == null
                        || ( startKeyDocId != null && id.equals(startKeyDocId) ) )) {
                    offset = i;
                }
                if (key.equals(endKey)
                        && ( endKeyDocId == null
                        || ( endKeyDocId != null && id.equals(endKeyDocId) ) )) {
                    if (!config.isInclusiveEnd()) {
                        end = i - 1;
                        break;
                    } else {
                        end = i;
                    }
                }
            }
            while (end < rows.size() - 1) {
                rows.remove(rows.size() - 1);
            }
        }

        /* pagination: skip, limit */
        offset += config.getSkip();
        if (offset > rows.size()) {
            offset = rows.size();
        }
        for (int i = 0; i < offset; ++i) {
            rows.remove(0);
        }
        if (config.getLimit() != null) {
            while (rows.size() > config.getLimit()) {
                rows.remove(rows.size() - 1);
            }
        }

        /* filtering: key, keys */
        ArrayList filter = new ArrayList();
        if (config.getKey() != null) {
            filter.add(config.getKey());
        }
        if (config.getKeys() != null) {
            filter.addAll(config.getKeys());
        }
        if (!filter.isEmpty()) {
            for (int i = 0; i < filter.size(); ++i) {
                try {
                    filter.set(i, JSONSerializer.toJSON(filter.get(i)));
                } catch (JSONException ex) {
                }
            }
            ArrayList<HashMap> filtered = new ArrayList<HashMap>();
            for (HashMap row : rows) {
                if (filter.contains(row.get("key"))) {
                    filtered.add(row);
                }
            }
            rows = filtered;
        }

        HashMap response = new HashMap();
        response.put("total_rows", totalRows);
        response.put("offset", offset);
        response.put("rows", rows);
        return response;
    }

    public static Object fromNativeObject(Object object) {
        if (object instanceof NativeArray) {
            NativeArray array = (NativeArray) object;
            int length = (int) array.getLength();
            ArrayList json = new ArrayList();
            for (int i = 0; i < length; i++) {
                json.add(fromNativeObject(ScriptableObject.getProperty(array, i)));
            }
            return json;
        } else if (object instanceof ScriptableObject) {
            ScriptableObject scriptable = (ScriptableObject) object;
            HashMap json = new HashMap();

            Object[] ids = scriptable.getAllIds();
            for (Object id : ids) {
                String key = id.toString();
                Object property = ScriptableObject.getProperty(scriptable, key);
                Object value = null;
                if (property instanceof Scriptable && ( (Scriptable) property ).getClassName().equals("Date")) {
                    // Convert NativeDate to Date

                    // (The NativeDate class is private in Rhino, but we can access
                    // it like a regular object.)
                    Object time = ScriptableObject.callMethod(scriptable, "getTime", null);
                    if (time instanceof Number) {
                        value = DATE_FORMAT.format(new Date(( (Number) time ).longValue()));
                    }
                } else {
                    value = fromNativeObject(property);
                }
                json.put(key, value);
            }
            return json;
        } else if (object instanceof Integer) {
            return ( (Number) object ).longValue();
        } else if (object instanceof Double || object instanceof Float) {
            Double d = ( (Number) object ).doubleValue();
            if (d % 1 == 0.0) {
                return d.longValue();
            }
            return object;
        } else {
            return object;
        }
    }

    public static Object parseJSON(Object object) {
        if (object == null || JSONUtils.isNumber(object) || JSONUtils.isBoolean(object)) {
            return object;
        } else {
            try {
                return JSONSerializer.toJSON(object);
            } catch (JSONException e1) {
                try {
                    return Long.parseLong((String) object);
                } catch (NumberFormatException e2) {
                    try {
                        return Double.parseDouble((String) object);
                    } catch (NumberFormatException e3) {
                        return object;
                    }
                }
            }
        }
    }
}
