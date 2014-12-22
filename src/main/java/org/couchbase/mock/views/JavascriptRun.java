package org.couchbase.mock.views;

import org.couchbase.mock.util.ReaderUtils;
import org.mozilla.javascript.*;

import java.io.IOException;

public class JavascriptRun {
    final private static String VIEWIDXR_JS;
    final private static String COLLATE_JS;
    final private Scriptable scope;
    final private Function execFunc;

    static {
        try {
            VIEWIDXR_JS = ReaderUtils.fromResource("views/viewidxr.js");
            COLLATE_JS = ReaderUtils.fromResource("views/collate.js");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public JavascriptRun() {
        Context cx = Context.enter();
        scope = new ImporterTopLevel(cx);
        try {
            cx.evaluateString(scope, COLLATE_JS, "collate.js", 1, null);
            cx.evaluateString(scope, VIEWIDXR_JS, "viewidxr.js", 1, null);
        } finally {
            Context.exit();
        }

        execFunc = (Function) scope.get("execute", scope);
    }

    NativeObject execute(NativeObject options, Scriptable mappedRows, Scriptable reducer, Context cx) {
        Object[] args = new Object[] { options, mappedRows, reducer };
        if (args[2] == null) {
            args[2] = Undefined.instance;
        }
        return (NativeObject) execFunc.call(cx, scope, scope, args);
    }

    public static String getCollateJS() {
        return COLLATE_JS;
    }
}