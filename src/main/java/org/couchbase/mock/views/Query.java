package org.couchbase.mock.views;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.Undefined;

import java.util.Map;

public class Query {
    final private Indexer indexer;
    final private Reducer reducer;
    final private NativeObject options;
    final private JavascriptRun runner;

    public Query(Indexer indexer, Reducer reducer, JavascriptRun runner, Map<String,String> qOptions) {
        this.options = new NativeObject();
        this.indexer = indexer;
        this.reducer = reducer;
        this.runner = runner;

        for (Map.Entry<String,String> entry : qOptions.entrySet()) {
            options.put(entry.getKey(), options, entry.getValue());
        }
    }

    NativeObject executeJson() {
        Context cx = Context.enter();
        Scriptable redFunc;
        if (reducer == null) {
            redFunc = null;
        } else {
            redFunc = reducer.getFunction();
        }
        try {
            return runner.execute(options, indexer.getLastResults(), redFunc, cx);
        } finally {
            Context.exit();
        }
    }
}
