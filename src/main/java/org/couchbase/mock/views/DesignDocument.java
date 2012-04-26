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
import java.util.Set;
import javax.script.ScriptException;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;

/**
 *
 * @author Sergey Avseyev
 */
public class DesignDocument {

    private String body;
    private String id;
    private ArrayList<View> views;

    public DesignDocument(String body) {
        try {
            this.body = body;
            JsonConfig cfg = new JsonConfig();
            cfg.clearJsonValueProcessors();
            JSONObject json = (JSONObject) JSONObject.fromObject(body, cfg);
            this.id = json.getString("_id");
            this.views = new ArrayList<View>();
            JSONObject viewsJson = (JSONObject) json.getJSONObject("views");
            for (String viewName : (Set<String>) viewsJson.keySet()) {
                JSONObject view = (JSONObject) viewsJson.getJSONObject(viewName);
                String mapSrc = view.getString("map");
                String reduceSrc = null;
                try {
                    reduceSrc = view.getString("reduce");
                } catch (JSONException _) {
                    // let it null
                }
                views.add(new View(viewName, mapSrc, reduceSrc));
            }
        } catch (ScriptException ex) {
        } catch (JSONException ex) {
            throw new IllegalArgumentException("Incomplete document body", ex);
        }
    }

    public String getBody() {
        return body;
    }

    public String getId() {
        return id;
    }

    public ArrayList<View> getViews() {
        return views;
    }
}
