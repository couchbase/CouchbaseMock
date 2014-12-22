package org.couchbase.mock.views;

import org.couchbase.mock.util.ReaderUtils;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ImporterTopLevel;
import org.mozilla.javascript.Scriptable;

import java.io.IOException;

public class Reducer {
    private final Scriptable scope;
    private final Function reduceFunc;

    private final static String REDUCE_JS;
    static {
        try {
            REDUCE_JS = ReaderUtils.fromResource("views/reduce.js");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Reducer(String reduceTxt, Context cx) {
        scope = new ImporterTopLevel(cx);
        cx.evaluateString(scope, REDUCE_JS, "reduce.js", 1, null);

        Scriptable builtins = (Scriptable)scope.get("BUILTIN_REDUCERS", scope);
        if (builtins.has(reduceTxt, builtins)) {
            reduceFunc = (Function)builtins.get(reduceTxt, builtins);
        } else {
            reduceFunc = cx.compileFunction(scope, reduceTxt, "reduce", 1, null);
        }
    }

    public static Reducer create(String txt) {
        Context cx = Context.enter();
        try {
            return new Reducer(txt, cx);
        } finally {
            Context.exit();
        }
    }

    public Function getFunction() {
        return reduceFunc;
    }
}