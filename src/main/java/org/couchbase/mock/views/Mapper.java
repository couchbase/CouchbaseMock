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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.couchbase.mock.memcached.DataStore;
import org.couchbase.mock.memcached.Item;

/**
 *
 * @author Sergey Avseyev
 */
public class Mapper {

    private final String body;
    private final ScriptEngine engine;

    public Mapper(ScriptEngine engine, String body) {
        this.engine = engine;
        this.body = body;
    }

    public ArrayList<HashMap> execute(DataStore store, Configuration config) {
        ArrayList<HashMap> rows = new ArrayList<HashMap>();

        for (Map<String, Item> map : store.getData()) {
            for (Map.Entry<String, Item> entry : map.entrySet()) {
                String jsonStr = new String(entry.getValue().getValue());
                try { /* ensure that value is JSON document */
                    JSONObject json = JSONObject.fromObject(jsonStr);
                    json.put("_id", entry.getKey());
                    jsonStr = json.toString();
                } catch (JSONException ex) {
                    continue;
                }
                try {
                    engine.eval("result = []");
                    engine.eval("(" + body + ")(" + jsonStr + ")");
                    ArrayList<ArrayList> result = (ArrayList<ArrayList>) View.fromNativeObject(engine.get("result"));
                    for (ArrayList row : result) {
                        HashMap document = new HashMap();
                        document.put("id", entry.getKey());
                        document.put("key", row.get(0));
                        document.put("value", row.get(1));
                        if (config.includeDocs()) {
                            Item item = entry.getValue();
                            JSONObject obj = JSONObject.fromObject(new String(item.getValue()));
                            obj.put("$flags", item.getFlags());
                            obj.put("$exp", item.getExptime());
                            document.put("doc", obj);
                        }
                        rows.add(document);
                    }
                } catch (ScriptException ex) {
                    Logger.getLogger(View.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return rows;
    }
}
