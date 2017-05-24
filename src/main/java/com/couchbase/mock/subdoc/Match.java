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

package com.couchbase.mock.subdoc;

import com.google.gson.*;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is responsible for actually matching the path against the JSON document
 * and determining if
 */
public class Match {
    private final static Gson gs = new Gson();
    // Deepest index (in path) that was found

    // Chain of JSON elements leading up to the deepest match;
    private List<JsonElement> chain = new ArrayList<JsonElement>();

    // Document to search
    private final JsonElement root;

    // Path to search for
    private final Path path;

    public Match(JsonElement root, Path path) {
        this.root = root;
        this.path = path;

        chain.add(root);
    }

    public static Match match(String json, String path) throws SubdocException {
        JsonElement e;
        try {
            e = gs.fromJson(json, JsonElement.class);
        } catch (JsonSyntaxException ex) {
            throw new DocNotJsonException(ex);
        }
        return match(e, new Path(path));
    }

    public static Match match(JsonElement root, Path path) throws SubdocException {
        Match m = new Match(root, path);
        m.execute();
        return m;
    }

    public void execute() throws SubdocException {
        JsonElement parent = root;

        for (int i = 0; i < path.size(); i++) {
            path.validateComponentType(i, parent);
            Component component = path.get(i);

            if (component.isIndex()) {
                int index = component.getIndex();
                JsonArray array = parent.getAsJsonArray();
                if (array.size() == 0) {
                    // Empty array
                    break;
                }

                if (index == -1) {
                    index = array.size() - 1;
                } else if (index > array.size()-1) {
                    break; // Not found!
                }
                parent = array.get(index);
            } else {
                JsonObject object = parent.getAsJsonObject();
                parent = object.get(component.getString());
            }

            if (parent == null) {
                // Match not found here!
                break;
            }

            chain.add(parent);
        }
    }

    public boolean isFound() {
        if (path.size() == 0) {
            return true;
        }
        return chain.size() - 1 == path.size();
    }

    public boolean hasImmediateParent() {
        if (path.size() == 0) {
            return true;
        }
        return (chain.size() - 1) >= path.size()-1;
    }

    public JsonElement getRoot() {
        return root;
    }

    List<JsonElement> getChain() {
        return chain;
    }

    JsonElement getDeepest() {
        return chain.get(chain.size()-1);
    }

    JsonElement getMatchParent() {
        if (!isFound()) {
            throw new IllegalStateException("No match found. Cannot get parent");
        }
        return chain.get(chain.size()-2);
    }

    JsonElement getImmediateParent() {
        if (isFound()) {
            return getMatchParent();
        } else if (hasImmediateParent()) {
            return getDeepest();
        } else {
            throw new IllegalStateException("No match or immediate parent!");
        }
    }

    JsonElement getMatch() {
        if (!isFound()) {
            throw new IllegalStateException("No match found!");
        }
        return getDeepest();
    }
}