package org.couchbase.mock.views;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.couchbase.mock.JsonUtils;
import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.util.ReaderUtils;
import org.mozilla.javascript.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for indexer. This is run on every document.
 */
public class Indexer {
    private static final String INDEX_JS;
    private static final Object[] NO_ARGS = new Object[] {};

    static {
        try {
            INDEX_JS = ReaderUtils.fromResource("views/index.js");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final Scriptable scope;
    private final Function mapFunction;
    private final Function indexFunction;
    private Scriptable indexResults = null;

    private Indexer(String mapTxt, Context cx) {
        scope = new ImporterTopLevel(cx);
        cx.evaluateString(scope, INDEX_JS, "index.js", 1, null); // Index source
        cx.evaluateString(scope, JavascriptRun.getCollateJS(), "collate.js", 1, null); // Collation
        mapFunction = cx.compileFunction(scope, mapTxt, "map", 1, null);

        // var index = new Index()
        indexResults = cx.newObject(scope, "Index");

        indexFunction = (Function) indexResults.getPrototype().get("indexDoc", indexResults);

        // var emit = index.emit
        Function emitFunc = (Function) indexResults.getPrototype().get("emit", indexResults);
        emitFunc = new BoundFunction(cx, scope, emitFunc, indexResults, NO_ARGS);
        scope.put("emit", scope, emitFunc);
    }

    public void run(Iterable<Item> items, Context cx) {
        Function prepareFunc = (Function) indexResults.getPrototype().get("prepare", indexResults);
        prepareFunc.call(cx, scope, indexResults, NO_ARGS);

        Object args[] = new Object[] { null, mapFunction };
        for (Item item : items) {
            args[0] = item;
            indexFunction.call(cx, scope, indexResults, args);
        }

        Function doneFunc = (Function) indexResults.getPrototype().get("setDone", indexResults);
        doneFunc.call(cx, scope, indexResults, NO_ARGS);
    }

    public static Indexer create(String mapTxt) {
        Context cx = Context.enter();
        try {
            return new Indexer(mapTxt, cx);
        } finally {
            Context.exit();
        }
    }

    Scriptable getLastResults() {
        return indexResults;
    }

    public static void main(String[] argv) {
        List<Item> items = new ArrayList<Item>();
        Map<String,Object> jsonMap = new HashMap<String,Object>();
        for (int i = 0; i < 10; i++) {
            String key = "Key_" + i;
            jsonMap.put("name", "Name" + i);
            jsonMap.put("number", i);

            Item item = new Item(key, JsonUtils.encode(jsonMap).getBytes());
            items.add(item);
        }

        items.add(new Item("binary key", new byte[]{1,2,3}));
        Context cx = Context.enter();
        Scriptable scope = new ImporterTopLevel(cx);
        Indexer indexer = Indexer.create("function(doc,meta) { if (doc.name) { emit([doc.number % 2 == 0 ? true : false, doc.name], null); } }");
        indexer.run(items, cx);

        // we should have a 'results'
        NativeObject lastResults = (NativeObject) indexer.getLastResults().get("results", indexer.getLastResults());
        System.out.println(lastResults);
        String s = (String) NativeJSON.stringify(cx, scope, lastResults, null, null);
        System.out.println(s);

        System.out.printf("Now running REDUCE...%n");
        Reducer reducer = Reducer.create("_count");

        Map<String,String> options = new HashMap<String, String>();
        options.put("group_level", "1");
        JavascriptRun jsRun = new JavascriptRun();
        Query q = new Query(indexer, reducer, jsRun, options);
        Object queryResult = q.executeJson();

        String outString = (String) NativeJSON.stringify(cx, scope, queryResult, null, null);
        JsonObject obj = new Gson().fromJson(outString, JsonObject.class);
        JsonArray rows = obj.getAsJsonArray("rows");
        for (JsonElement rowElem : rows) {
            JsonObject row = rowElem.getAsJsonObject();
            System.out.println(row);
        }
    }
}