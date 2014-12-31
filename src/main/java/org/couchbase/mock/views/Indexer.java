package org.couchbase.mock.views;

import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.util.ReaderUtils;
import org.mozilla.javascript.*;

import java.io.IOException;

/**
 * This class maintains an index on all items within a bucket. It is first created when
 * the view {@link org.couchbase.mock.views.View} is created, and is updated as necessary
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

    /**
     * Run the indexer on the given iterable of items. This will attempt to apply some
     * optimizations to ensure that only items which need re-indexing are actually passed
     * to the map function.
     *
     * @param items The items to index
     * @param cx The current execution context
     */
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

    /**
     * Create a new indexer object
     * @param mapTxt The text of the map function
     */
    public static Indexer create(String mapTxt) {
        Context cx = Context.enter();
        try {
            return new Indexer(mapTxt, cx);
        } finally {
            Context.exit();
        }
    }

    /**
     * Get the underlying Javascript indexed rows, suitable for passing to
     * {@link org.couchbase.mock.views.JavascriptRun#execute(org.mozilla.javascript.NativeObject, org.mozilla.javascript.Scriptable, org.mozilla.javascript.Scriptable, org.mozilla.javascript.Context)}
     * @return The JavaScript index
     */
    Scriptable getLastResults() {
        return indexResults;
    }
}