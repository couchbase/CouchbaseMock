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
import com.google.gson.stream.JsonReader;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.io.StringReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Executor {
    public final static Gson gs = new Gson();
    private final Path path;
    private final Operation code;
    private final JsonElement value;
    private final boolean isCreate;
    private final boolean isMultiValue;
    private final Match match;

    private static <T> T parseStrictJson(String text, Class<T> klass) {
        try {
            JSONValue.parseWithException(text);
        } catch (ParseException ex) {
            throw new JsonSyntaxException(ex);
        } catch (NumberFormatException ex2) {
            // Ignore number formats. GSON uses BigInteger if it's too big anyway. It's perfectly valid JSON
        }
        JsonReader reader = new JsonReader(new StringReader(text));
        reader.setLenient(false);
        return gs.fromJson(reader, klass);
    }

    private Executor(String input, Path path, Operation code, String valueFragment, boolean shouldCreateParents)
            throws SubdocException {
        this.path = path;
        this.code = code;
        this.isCreate = shouldCreateParents;

        if (code.requiresValue()) {
            if (valueFragment == null || valueFragment.isEmpty()) {
                throw new EmptyValueException();
            }

            try {
                if (code.isArrayParent()) {
                    valueFragment = "[" + valueFragment + "]";
                    JsonArray arr = parseStrictJson(valueFragment, JsonArray.class);
                    if (arr.size() > 1) {
                        value = arr;
                        isMultiValue = true;
                    } else {
                        value = arr.get(0);
                        isMultiValue = false;
                    }
                } else {
                    valueFragment = "{\"K\":" + valueFragment + "}";
                    JsonObject obj = parseStrictJson(valueFragment, JsonObject.class);
                    if (obj.getAsJsonObject().entrySet().size() != 1) {
                        throw new CannotInsertException("More than one value found in object!");
                    }
                    value = obj.get("K");
                    isMultiValue = false;
                }
            } catch (JsonSyntaxException ex) {
                if (code == Operation.COUNTER) {
                    throw new BadNumberException(ex);
                } else {
                    throw new CannotInsertException(ex);
                }
            }
        } else {
            value = null;
            isMultiValue = false;
        }

        if (isMultiValue && !code.allowsMultiValue()) {
            throw new CannotInsertException("Multi value not allowed!");
        }

        JsonElement root;
        try {
            root = parseStrictJson(input, JsonElement.class);
        } catch (JsonSyntaxException e) {
            throw new DocNotJsonException(e);
        }

        match = new Match(root, path);
    }

    public static JsonElement executeGet(String input, String path) throws SubdocException {
        return execute(input, path, Operation.GET).getMatch();
    }

    public static Result execute(String input, String path, Operation code) throws SubdocException {
        return execute(input, new Path(path), code);
    }

    public static Result execute(String input, String path, Operation code, String valueFragment, boolean isMkdirP)
            throws SubdocException {
        return execute(input, new Path(path), code, valueFragment, isMkdirP);
    }

    public static Result execute(String input, String path, Operation code, String valueFragment)
            throws SubdocException {
        return execute(input, path, code, valueFragment, false);
    }

    public static Result execute(String input, Path path, Operation code) throws SubdocException {
        // For Get, Exists and Delete
        return execute(input, path, code, null, false);
    }

    public static Result execute(String input, Path path, Operation code, String valueFragment, boolean isMkdirP)
            throws SubdocException {
        Executor p = new Executor(input, path, code, valueFragment, isMkdirP);
        p.match.execute();
        return p.operate();
    }

    private void insertInJsonArray(JsonArray array, int index) {
        // Because JsonArray doesn't implement Collection or List, we need
        // to use a temporary list, and then reassemble the contents into
        // an array
        List<JsonElement> elements = new ArrayList<JsonElement>();
        while (array.size() > 0) {
            elements.add(array.remove(0));
        }

        List<JsonElement> newElements = new ArrayList<JsonElement>();
        if (isMultiValue) {
            for (JsonElement elem : value.getAsJsonArray()) {
                newElements.add(elem);
            }
        } else {
            newElements.add(value);
        }

        elements.addAll(index, newElements);
        for (JsonElement elem : elements) {
            array.add(elem);
        }
    }

    enum ParentType { ARRAY, OBJECT }

    private void createParents(ParentType parentType, JsonElement newValue) throws SubdocException {
        if (match.getDeepest().isJsonArray()) {
            throw new PathMismatchException("Cannot create intermediate array!");
        }

        List<JsonElement> chain = match.getChain();
        // Get the first missing component index. This is the size of the chain, less two
        int lastIndex = chain.size() - 2;

        for (int i = lastIndex + 1; i < path.size() - 1; i++) {
            Component comp = path.get(i);

            if (comp.isIndex()) {
                // Cannot insert elements with MKDIR_P
                throw new PathNotFoundException();
            }

            JsonObject nextParent = new JsonObject();
            JsonObject prevParent = chain.get(chain.size()-1).getAsJsonObject();
            chain.add(nextParent);
            prevParent.add(comp.getString(), nextParent);
        }

        Component lastComp = path.getLast();
        if (lastComp.isIndex()) {
            throw new PathNotFoundException();
        }

        JsonObject deepest = chain.get(chain.size()-1).getAsJsonObject();

        if (parentType == ParentType.ARRAY) {
            // ADD_UNIQUE, ARRAY_PREPEND, ARRAY_APPEND
            JsonArray parentArray = new JsonArray();
            parentArray.add(newValue);
            deepest.add(lastComp.getString(), parentArray);
        } else {
            deepest.add(lastComp.getString(), newValue);
        }
    }

    private void replace(JsonElement newValue) throws SubdocException {
        if (!match.isFound()) {
            throw new PathNotFoundException();
        } else if (path.size() == 0) {
            throw new CannotInsertException("Cannot replace root element!");
        }

        JsonElement parent = match.getMatchParent();
        Component comp = path.getLast();

        if (comp.isIndex()) {
            int index = comp.getIndex();
            JsonArray array = parent.getAsJsonArray();
            if (index == -1) {
                index = parent.getAsJsonArray().size()-1;
            }
            array.set(index, value);
        } else {
            parent.getAsJsonObject().add(comp.getString(), newValue);
        }
    }

    private JsonElement dictAdd(JsonElement newValue) throws SubdocException {
        if (match.isFound()) {
            throw new PathExistsException();
        }

        if (!match.hasImmediateParent()) {
            if (!isCreate) {
                throw new PathNotFoundException();
            }
            createParents(ParentType.OBJECT, newValue);
            return match.getRoot();
        }

        Component lastComp = path.getLast();
        JsonElement parent = match.getImmediateParent();
        if (!parent.isJsonObject()) {
            throw new PathMismatchException("DICT_ADD must have dictionary parent");
        }
        parent.getAsJsonObject().add(lastComp.getString(), newValue);
        return match.getRoot();
    }

    private void ensureUnique(JsonArray array) throws SubdocException {
        if (!value.isJsonPrimitive() && !value.isJsonNull()) {
            throw new CannotInsertException("Cannot verify uniqueness with non-primitives");
        }

        String valueString = value.toString();
        for (int i = 0; i < array.size(); i++) {
            JsonElement e = array.get(i);
            if (!e.isJsonPrimitive()) {
                throw new PathMismatchException("Values in the array are not all primitives");
            }
            if (e.toString().equals(valueString)) {
                throw new PathExistsException();
            }
        }
    }

    private void arrayAdd() throws SubdocException {
        if (!match.isFound()) {
            if (isCreate) {
                createParents(ParentType.ARRAY, value);
                return;
            } else {
                throw new PathNotFoundException();
            }
        }

        JsonElement lastElem = match.getDeepest();
        if (!lastElem.isJsonArray()) {
            throw new PathMismatchException();
        }

        JsonArray array = lastElem.getAsJsonArray();

        if (code == Operation.ADD_UNIQUE) {
            ensureUnique(array);
        }

        insertInJsonArray(array,
                code == Operation.ARRAY_APPEND ? array.size() : 0);
    }

    private void arrayInsert() throws SubdocException {
        JsonArray array;
        int position = path.getLast().getIndex();

        if (match.hasImmediateParent()) {
            array = match.getImmediateParent().getAsJsonArray();
        } else {
            throw new PathNotFoundException();
        }

        if (position == -1) {
            throw new InvalidPathException("Insert does not accept negative arrays");
        } else if (position > array.size()) {
            // Index is too big!
            throw new PathNotFoundException();
        } else {
            insertInJsonArray(array, position);
        }
    }

    private Result remove() throws SubdocException {
        if (!match.isFound()) {
            throw new PathNotFoundException();
        } else if (path.size() == 0) {
            throw new CannotInsertException("Cannot delete root element!");
        }

        JsonElement parent = match.getImmediateParent();
        JsonElement removedElement;
        Component lastComp = path.getLast();

        if (parent.isJsonObject()) {
            removedElement = parent.getAsJsonObject().remove(lastComp.getString());
        } else {
            JsonArray array = parent.getAsJsonArray();
            int index = lastComp.getIndex();
            if (index == -1) {
                index = array.size() - 1;
            }
            removedElement = array.remove(index);
        }

        return new Result(removedElement, match.getRoot());
    }

    static private boolean bigintIsWithinRange(BigInteger ee) {
        BigInteger longMax = new BigInteger(Long.toString(Long.MAX_VALUE));
        return ee.compareTo(longMax) <= 0;
    }

    private static JsonElement getCount(JsonElement elem) throws SubdocException {
        if (elem.isJsonObject()) {
            return new JsonPrimitive(elem.getAsJsonObject().entrySet().size());
        } else if (elem.isJsonArray()) {
            return new JsonPrimitive(elem.getAsJsonArray().size());
        } else {
            throw new PathMismatchException("GET_COUNT must point to array or dictionary");
        }
    }

    private JsonElement counter() throws SubdocException {
        Long numres;
        Long delta;

        try {
            BigInteger ee = value.getAsBigInteger();
            if (!bigintIsWithinRange(ee)) {
                throw new DeltaTooBigException();
            }
            delta = ee.longValue();
        } catch (NumberFormatException ex) {
            throw new BadNumberException(ex);
        } catch (UnsupportedOperationException ex2) {
            throw new BadNumberException(ex2);
        }

        if (delta == 0) {
            throw new ZeroDeltaException();
        }

        if (match.isFound()) {
            try {
                BigInteger tmpCombo = match.getMatch().getAsBigInteger();
                if (!bigintIsWithinRange(tmpCombo)) {
                    throw new NumberTooBigException();
                }
                numres = tmpCombo.longValue();
            } catch (UnsupportedOperationException ex) {
                throw new PathMismatchException(ex);
            } catch (NumberFormatException ex2) {
                throw new PathMismatchException(ex2);
            }

            /*
            if (delta >= 0 && numres >= 0) {
                if (std::numeric_limits<int64_t>::max() - delta < numres) {
                    return Error::DELTA_E2BIG;
                }
            } else if (delta < 0 && numres < 0) {
                if (delta < std::numeric_limits<int64_t>::min() - numres) {
                    return Error::DELTA_E2BIG;
                }
            }
             */

            if (delta >= 0 && numres >= 0) {
                if (Long.MAX_VALUE - delta < numres) {
                    throw new DeltaTooBigException();
                }
            } else if (delta < 0  && numres < 0) {
                if (delta < Long.MIN_VALUE - numres) {
                    throw new DeltaTooBigException();
                }
            }

            JsonPrimitive p = new JsonPrimitive(numres + delta);
            replace(p);
            return p;
        } else {
            JsonPrimitive newNum = new JsonPrimitive(delta);
            if (match.hasImmediateParent() && match.getImmediateParent().isJsonObject()) {
                dictAdd(newNum);
            } else if (isCreate && match.getDeepest().isJsonObject()) {
                createParents(ParentType.OBJECT, newNum);
            } else {
                throw new PathNotFoundException();
            }
            return newNum;
        }
    }

    private Result operate() throws SubdocException {
        switch (code) {
            case GET:
            case EXISTS:
            case GET_COUNT:
                if (!match.isFound()) {
                    throw new PathNotFoundException();
                } else {
                    if (code == Operation.GET_COUNT) {
                        return new Result(getCount(match.getMatch()), null);
                    } else {
                        return new Result(match.getMatch(), null);
                    }
                }

            case GET_FULLDOC:
                return new Result(match.getRoot(), null);

            case REPLACE:
                replace(value);
                return new Result(null, match.getRoot());

            case DICT_UPSERT:
                if (path.getLast().isIndex()) {
                    throw new InvalidPathException("DICT_UPSERT cannot have an array index as its last component");
                }
                if (match.isFound()) {
                    replace(value);
                } else {
                    dictAdd(value);
                }

                return new Result(null, match.getRoot());

            case DICT_ADD:
                dictAdd(value);
                return new Result(null, match.getRoot());

            case ARRAY_PREPEND:
            case ARRAY_APPEND:
            case ADD_UNIQUE:
                arrayAdd();
                return new Result(null, match.getRoot());

            case ARRAY_INSERT:
                arrayInsert();
                return new Result(null, match.getRoot());

            case REMOVE:
                return remove();

            case COUNTER:
                return new Result(counter(), match.getRoot());

            case WRITE_FULLDOC:
                return new Result(null, value);

            default:
                throw new RuntimeException("Unknown operation!");
        }
    }

    public static String getRootType(String path, Operation op) {
        if (path.isEmpty()) {
            switch (op) {
                case ARRAY_APPEND:
                case ARRAY_PREPEND:
                case ADD_UNIQUE:
                    return "[]";
                default:
                    return null;
                case WRITE_FULLDOC:
                    return "{}";
            }
        }

        if (path.charAt(0) == '[') {
            return "[]";
        } else {
            return "{}";
        }
    }
}
