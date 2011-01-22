/**
 *     Copyright 2011 Membase, Inc.
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
package org.membase.jmembase.util;

import java.io.PrintWriter;

/**
 * Small utility class to make the escaping a little less pain ;-)
 * 
 * @author Trond Norbye
 */
public class JSON {

    public static void addElement(PrintWriter pw, String key, String value, boolean comma) {
        pw.print('"');
        pw.print(key);
        pw.print("\":\"");
        pw.print(value);
        pw.print('\"');
        if (comma) {
            pw.print(',');
        }
    }

    public static void addElement(PrintWriter pw, String key, int value, boolean comma) {
        pw.print('"');
        pw.print(key);
        pw.print("\":");
        pw.print(value);
        if (comma) {
            pw.print(',');
        }
    }

    private JSON() {
    }
}
