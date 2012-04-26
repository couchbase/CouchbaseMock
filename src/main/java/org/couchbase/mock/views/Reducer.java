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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONSerializer;

/**
 *
 * @author Sergey Avseyev
 */
public class Reducer {

    private final String body;
    private final ScriptEngine engine;

    public Reducer(ScriptEngine engine, String body) {
        this.engine = engine;
        this.body = body;
    }

    public ArrayList execute(ArrayList rows, Configuration config) throws ScriptException {
        if (body.startsWith("_count")) {
            return executeCount(rows, config);
        } else if (body.startsWith("_sum")) {
            return executeSum(rows, config);
        } else if (body.startsWith("_stats")) {
            return executeStats(rows, config);
        } else {
            return executeJS(rows, config);
        }
    }

    private class ReduceEntry {

        public ArrayList keys = new ArrayList();
        public ArrayList values = new ArrayList();

        public void add(Object key, Object id, Object value) {
            JSONArray json = new JSONArray();
            json.add(key);
            json.add(id);
            keys.add(json);
            values.add(value);
        }
    }

    private HashMap<Object, ReduceEntry> toReduceParams(ArrayList rows, Configuration config) {
        HashMap<Object, ReduceEntry> res = new HashMap<Object, ReduceEntry>();

        for (HashMap row : (ArrayList<HashMap>) rows) {
            Object key = null;
            if (config.isGroup()) {
                /* group by whole key */
                key = row.get("key");
            }
            if (config.getGroupLevel() > 0) {
                /* group by interval */
                JSON json = JSONSerializer.toJSON(row.get("key"));
                if (json instanceof JSONArray) {
                    ArrayList acc = new ArrayList();
                    for (int i = 0; i < config.getGroupLevel() && i < json.size(); ++i) {
                        acc.add(( (JSONArray) json ).get(i));
                    }
                    key = JSONArray.fromObject(acc).toString();
                }
            }
            if (!res.containsKey(key)) {
                res.put(key, new ReduceEntry());
            }
            res.get(key).add(row.get("key"), row.get("id"), row.get("value"));
        }

        return res;
    }

    private ArrayList executeJS(ArrayList rows, Configuration config) throws ScriptException {
        ArrayList res = new ArrayList();
        for (Entry<Object, ReduceEntry> entry : toReduceParams(rows, config).entrySet()) {
            JSON group = JSONSerializer.toJSON(entry.getKey());
            JSON params[] = new JSON[]{
                JSONSerializer.toJSON(entry.getValue().keys),
                JSONSerializer.toJSON(entry.getValue().values)
            };
            Object value = View.fromNativeObject(engine.eval("(" + body + ")(" + params[0].toString() + ", " + params[1].toString() + ", false)"));
            HashMap reduced = new HashMap();
            reduced.put("key", group);
            reduced.put("value", value);
            res.add(reduced);
        }
        return res;
    }

    private ArrayList executeCount(ArrayList rows, Configuration config) {
        ArrayList res = new ArrayList();
        for (Entry<Object, ReduceEntry> entry : toReduceParams(rows, config).entrySet()) {
            JSON group = JSONSerializer.toJSON(entry.getKey());
            HashMap reduced = new HashMap();
            reduced.put("key", group);
            reduced.put("value", entry.getValue().values.size());
            res.add(reduced);
        }
        return res;
    }

    private ArrayList executeSum(ArrayList rows, Configuration config) {
        ArrayList res = new ArrayList();
        for (Entry<Object, ReduceEntry> entry : toReduceParams(rows, config).entrySet()) {
            JSON group = JSONSerializer.toJSON(entry.getKey());
            HashMap reduced = new HashMap();
            reduced.put("key", group);
            double sum = 0;
            for (Object val : entry.getValue().values) {
                sum += ( (Number) val ).doubleValue();
            }
            reduced.put("value", sum);
            res.add(reduced);
        }
        return res;
    }

    private ArrayList executeStats(ArrayList rows, Configuration config) {
        ArrayList res = new ArrayList();
        for (Entry<Object, ReduceEntry> entry : toReduceParams(rows, config).entrySet()) {
            JSON group = JSONSerializer.toJSON(entry.getKey());
            HashMap reduced = new HashMap();
            reduced.put("key", group);
            ArrayList values = entry.getValue().values;
            double sum = 0;
            int count = values.size();
            double min = 0;
            double max = 0;
            double sumsqr = 0;
            if (count > 0) {
                min = max = ( (Number) values.get(0) ).doubleValue();
            }
            for (Object val : entry.getValue().values) {
                double d = ( (Number) val ).doubleValue();
                sum += d;
                sumsqr += Math.pow(d, 2);
                if (d < min) {
                    min = d;
                }
                if (d > max) {
                    max = d;
                }
            }
            HashMap stats = new HashMap();
            stats.put("sum", sum);
            stats.put("max", max);
            stats.put("min", min);
            stats.put("count", count);
            stats.put("sumsqr", sumsqr);
            reduced.put("value", stats);
            res.add(reduced);
        }
        return res;
    }
}
