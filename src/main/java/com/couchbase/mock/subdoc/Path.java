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

import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.List;

public class Path {
    final List<Component> components = new ArrayList<Component>();
    public final static Component ROOT = new Component();
    public final static int MAX_DEPTH = 32;

    private StringBuilder addComponent(StringBuilder sb, boolean isIndex) throws PathParseException, PathTooDeepException {
        if (components.size() == MAX_DEPTH-1) {
            throw new PathTooDeepException();
        }
        components.add(new Component(sb.toString(), isIndex));
        return new StringBuilder();
    }

    private boolean prevWasIndex() {
        return !components.isEmpty() && components.get(components.size()-1).isIndex();
    }

    public Path(String input) throws PathParseException, PathTooDeepException {
        boolean wantIndex = false;
        boolean inEscape = false;
        int numEscaped = 0;

        StringBuilder sb = new StringBuilder();
        for (char s : input.toCharArray()) {
            if (s == '`') {
                if (inEscape) {
                    inEscape = false;
                    if (numEscaped == 0) {
                        sb.append('`');
                    }
                } else {
                    inEscape = true;
                    numEscaped = 0;
                }
                continue;
            }

            if (inEscape) {
                numEscaped++;
                sb.append(s);
                continue;
            }

            if (s == '[') {
                if (wantIndex) {
                    throw new PathParseException("Found nested brackets!");
                }
                if (!sb.toString().isEmpty()) {
                    sb = addComponent(sb, false);
                }
                wantIndex = true;
            } else if (s == ']') {
                if (!wantIndex) {
                    throw new PathParseException("Found ] without opening [");
                }

                sb = addComponent(sb, true);
                wantIndex = false;

            } else if (s == '.') {
                if (!prevWasIndex()) {
                    sb = addComponent(sb, false);
                    wantIndex = false;
                }
            } else {
                sb.append(s);
            }
        }

        if (wantIndex) {
            throw new PathParseException("Found unclosed [");
        }

        String lastComp = sb.toString();
        if (lastComp.isEmpty()) {
            if (!components.isEmpty() && !prevWasIndex()) {
                throw new PathParseException("Found empty non-root component!");
            }
        } else {
            components.add(new Component(lastComp, false));
        }
    }

    public Component get(int ix) {
        return components.get(ix);
    }

    public Component getLast() {
        if (components.isEmpty()) {
            return ROOT;
        }
        return components.get(components.size()-1);
    }

    public int size() {
        return components.size();
    }

    public void validateComponentType(int ix, JsonElement parent) throws PathMismatchException {
        Component comp = components.get(ix);
        if (parent.isJsonPrimitive() || parent.isJsonNull()) {
            throw new PathMismatchException();
        }

        if (comp.isIndex()) {
            if (!parent.isJsonArray()) {
                throw new PathMismatchException();
            }
        } else {
            if (!parent.isJsonObject()) {
                throw new PathMismatchException();
            }
        }
    }
}
