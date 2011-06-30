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

import org.couchbase.mock.util.Base64;

import junit.framework.TestCase;

/**
 * Test that the utility functions in Base64 works as expected.
 *
 * @author Trond Norbye
 */
public class Base64Test extends TestCase {
    
    public Base64Test(String testName) {
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

    /**
     * Test of encode method, of class Base64.
     */
    public void testEncode() {
        System.out.println("encode");
        String input = "Aladdin:open sesame";
        String expResult = "QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
        String result = Base64.encode(input);
        assertEquals(expResult, result);
    }

    /**
     * Test of decode method, of class Base64.
     */
    public void testDecode() {
        System.out.println("decode");
        String input = "QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
        String expResult = "Aladdin:open sesame";
        String result = Base64.decode(input);
        assertEquals(expResult, result);
    }

    public void testBubbaTheHut() {
        assertEquals("Bubba:TheHut", Base64.decode(Base64.encode("Bubba:TheHut")));
    }
}
