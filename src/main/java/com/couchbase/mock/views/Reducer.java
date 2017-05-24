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

package com.couchbase.mock.views;

import com.couchbase.mock.util.ReaderUtils;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ImporterTopLevel;
import org.mozilla.javascript.Scriptable;

import java.io.IOException;

/**
 * Class representing a compiled reduce function. This class ensures to compile
 * the reduce function so that the various Couchbase-specific builtins will
 * function properly.
 */
public class Reducer {
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
        Scriptable scope = new ImporterTopLevel(cx);
        cx.evaluateString(scope, REDUCE_JS, "reduce.js", 1, null);

        Scriptable builtins = (Scriptable) scope.get("BUILTIN_REDUCERS", scope);
        if (builtins.has(reduceTxt, builtins)) {
            reduceFunc = (Function)builtins.get(reduceTxt, builtins);
        } else {
            reduceFunc = cx.compileFunction(scope, reduceTxt, "reduce", 1, null);
        }
    }

    /**
     * Create a new Reducer object
     * @param txt The raw reduce JavaScript source
     * @return The compiled reduce function
     */
    public static Reducer create(String txt) {
        Context cx = Context.enter();
        try {
            return new Reducer(txt, cx);
        } finally {
            Context.exit();
        }
    }

    /**
     * Get the actual compiled Rhino function, suitable for execution
     * @return The compiled rhino function
     */
    public Function getFunction() {
        return reduceFunc;
    }
}