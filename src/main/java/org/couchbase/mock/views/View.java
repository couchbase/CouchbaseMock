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

import org.couchbase.mock.JsonUtils;
import org.couchbase.mock.memcached.Item;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mozilla.javascript.*;

import javax.script.ScriptException;

/**
 * This represents a compiled Couchbase View that is part of the bucket. A view
 * consists of a {@link org.couchbase.mock.views.DesignDocument} consisting of a
 * {@code map} function and optionally a {@code reduce} function.
 *
 * @author Sergey Avseyev
 * @author Mark Nunberg
 */
public class View {
    private final String name;
    private final String mapSource;
    private final String reduceSource;
    private final Indexer indexer;
    private final Reducer reducer;
    private final JavascriptRun jsRun;


    public View(String name, String map) throws ScriptException {
        this(name, map, null);
    }

    /**
     * Create a new view
     * @param name The name of the view
     * @param map The JavaScript map function as a String
     * @param reduce The JavaScript reduce function, as a String
     * @throws org.mozilla.javascript.EcmaError if the map or reduce functions could not be parsed
     */
    public View(@NotNull String name, @NotNull String map, @Nullable String reduce) throws ScriptException {
        this.name = name;
        this.mapSource = map;
        this.reduceSource = reduce;

        this.jsRun = new JavascriptRun();
        this.indexer = Indexer.create(map);
        if (reduce != null) {
            this.reducer = Reducer.create(reduce);
        } else {
            this.reducer = null;
        }
    }

    /**
     * Gets the name of the view
     * @return The name of the view
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the JavaScript source code for the map function
     * @return The map source code
     */
    public String getMapSource() {
        return mapSource;
    }

    /**
     * Gets the JavaScript source code for the reduce function
     * @return The reduce source code
     */
    public String getReduceSource() {
        return reduceSource;
    }

    public QueryResult execute(Iterable<Item> items) throws QueryExecutionException {
        return execute(items, null);
    }

    /**
     * Executes the view query with the given parameters
     * @param items The items in the bucket which should be processed via the view
     * @param config The configuration for the view, acting as a filter on the items processed
     * @return A results object which may be inspected
     * @throws QueryExecutionException If there was an error while processing the options
     * @see {@link #executeRaw(Iterable, Configuration)}
     */
    public QueryResult execute(Iterable<Item> items, Configuration config) throws QueryExecutionException {
        return new QueryResult(JsonUtils.decodeAsMap(executeRaw(items, config)));
    }

    /**
     * Executes the view query with the given parameters.
     * @param items The items to be indexed
     * @param config The configuration to use for filters
     * @return A string suitable for returning to a Couchbase client
     * @throws QueryExecutionException
     */
    public String executeRaw(Iterable<Item> items, Configuration config) throws QueryExecutionException {
        if (config == null) {
            config = new Configuration();
        }

        Context cx = Context.enter();
        Scriptable scope = cx.initStandardObjects();
        NativeObject configObject = config.toNativeObject();

        Scriptable redFunc = null;
        if (reducer != null) {
            redFunc = reducer.getFunction();
        }

        try {
//            long indexStart = System.currentTimeMillis();
            indexer.run(items, cx);
//            long indexEnd = System.currentTimeMillis();
//            System.err.printf("Indexing took %d ms%n", indexEnd-indexStart);

            Scriptable indexResults = indexer.getLastResults();
            Scriptable resultObject;

            try {
//                long filterStart = System.currentTimeMillis();
                resultObject = jsRun.execute(configObject, indexResults, redFunc, cx);
//                long filterEnd = System.currentTimeMillis();
//                System.err.printf("Filtering took %d ms%n", filterEnd-filterStart);
            } catch (JavaScriptException ex) {
                Object thrownObject = ex.getValue();
                String jsonException;
                try {
                    jsonException = (String) NativeJSON.stringify(cx, scope, thrownObject, null, null);
                    throw new QueryExecutionException(jsonException);
                } catch (EcmaError ex2) {

                    throw new QueryExecutionException(ex2.getErrorMessage());
                }
            } catch (EcmaError parseErr) {
                throw new QueryExecutionException(parseErr.getErrorMessage());
            }

            NativeArray rows = (NativeArray) resultObject.get("rows", resultObject);
            resultObject.delete("rows");

            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (Object id : ((NativeObject)resultObject).getAllIds()) {
                if (! (id instanceof String)) {
                    throw new RuntimeException("ARGH: " + id);
                }
                sb.append('"').append(id).append("\":");
                sb.append((String)NativeJSON.stringify(cx, scope, resultObject.get((String)id, resultObject), null, null));
                sb.append(",");
            }

            sb.append("\"rows\":[\n");
            for (int i = 0; i < rows.size(); i++) {
                Object o = rows.get(i, rows);
                sb.append((String)NativeJSON.stringify(cx, scope, o, null, null));
                if (i < rows.size()-1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append("]\n");
            sb.append("}\n");
            return sb.toString();
        } finally {
            Context.exit();
        }
    }
}