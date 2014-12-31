package org.couchbase.mock.views;

import org.couchbase.mock.util.ReaderUtils;
import org.jetbrains.annotations.Nullable;
import org.mozilla.javascript.*;

import java.io.IOException;

/**
 * Represents the JavaScript flow for view query execution. This object should be created once per view
 */
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

    /**
     * @param options retrieved via {@link Configuration#toNativeObject()}
     * @param mappedRows Indexed rows. See {@link org.couchbase.mock.views.Indexer#}
     * @param reducer The reduced function. May be null
     * @param cx The current execution context
     * @return The raw result set
     */
    NativeObject execute(NativeObject options, Scriptable mappedRows, @Nullable Scriptable reducer, Context cx) {
        Object[] args = new Object[] { options, mappedRows, reducer };
        if (args[2] == null) {
            args[2] = Undefined.instance;
        }
        return (NativeObject) execFunc.call(cx, scope, scope, args);
    }

    /**
     * @return Retrieves the source code for the common JavaScript sorting/comparison routines
     */
    public static String getCollateJS() {
        return COLLATE_JS;
    }
}