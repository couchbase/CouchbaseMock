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
package org.couchbase.mock.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import junit.framework.TestCase;

/**
 * Basic testing of my little JSON helper utility
 *
 * @author Trond Norbye
 */
public class JSONTest extends TestCase {
    
    public JSONTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testAddElementStringWithoutComma() {
        System.out.println("AddElementStringWithoutComma");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        JSON.addElement(pw, "hello", "world", false);
        pw.flush();
        assertEquals("\"hello\":\"world\"", sw.toString());
    }

    public void testAddElementStringWithComma() {
        System.out.println("AddElementStringWithComma");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        JSON.addElement(pw, "hello", "world", true);
        pw.flush();
        assertEquals("\"hello\":\"world\",", sw.toString());
    }

    public void testAddElementIntWithoutComma() {
        System.out.println("AddElementIntWithoutComma");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        JSON.addElement(pw, "hello", 5, false);
        pw.flush();
        assertEquals("\"hello\":5", sw.toString());
    }

    public void testAddElementIntWithComma() {
        System.out.println("AddElementIntWithComma");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        JSON.addElement(pw, "hello", 5, true);
        pw.flush();
        assertEquals("\"hello\":5,", sw.toString());
    }
}
