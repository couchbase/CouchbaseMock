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
import org.mozilla.javascript.*;
import sun.security.krb5.Config;

import javax.script.ScriptException;
import java.util.Map;

/**
 *
 * @author Sergey Avseyev
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

    public View(String name, String map, String reduce) throws ScriptException {
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

    public String getName() {
        return name;
    }

    public String getMapSource() {
        return mapSource;
    }

    public String getReduceSource() {
        return reduceSource;
    }

    public QueryResult execute(Iterable<Item> items) throws QueryExecutionException {
        return execute(items, null);
    }

    public QueryResult execute(Iterable<Item> items, Configuration config) throws QueryExecutionException {
        return new QueryResult(JsonUtils.decodeAsMap(executeRaw(items, config)));
    }

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