/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.control;

import com.google.gson.Gson;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class CommandStatus {
    private boolean success = true;
    private String failMsg = null;
    private Throwable t = null;
    private Object payload = new HashMap<String, Object>();
    private Map<String,Object> header = new HashMap<String, Object>();

    public CommandStatus fail() {
        success = false;
        return this;
    }

    public CommandStatus fail(String reason) {
        success = false;
        failMsg = reason;
        return this;
    }

    public CommandStatus fail(Throwable exc) {
        success = false;
        t = exc;
        return this;
    }

    public void setPayload(Object o) {
        payload = o;
    }

    @Override
    public String toString() {
        header.put("payload", payload);
        if (success) {
            header.put("status", "ok");
        } else {
            header.put("status", "fail");
        }

        if (failMsg != null) {
            header.put("error", failMsg);
        } else if (t != null) {
            StringWriter sw = new StringWriter();
            sw.write("BEGIN CAUGHT EXCEPTION >>>\n");
            t.printStackTrace(new PrintWriter(sw));
            sw.write("<<< END CAUGHT EXCEPTION");
            header.put("error", sw.toString());
        }

        return new Gson().toJson(header).toString();
    }
}
