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

package com.couchbase.mock.subdoc;

import com.google.gson.JsonElement;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// Tests modelled after subjson/tests/t_ops.cc
public class ExecutorTest {
    private static String rootText;

    @BeforeClass
    public static void loadJsonTest() throws IOException {
        InputStream ss = ExecutorTest.class.getClassLoader().getResourceAsStream("subdoc/big_json.json");
        StringBuilder sb = new StringBuilder();
        InputStreamReader rr = new InputStreamReader(ss);
        char[] buffer = new char[4096];
        for (;;) {
            int nRead = rr.read(buffer, 0, buffer.length);
            if (nRead < 0) {
                break;
            }
            sb.append(buffer, 0, nRead);
        }
        rootText = sb.toString();
    }

    private static <T> void assertRaisesPriv(Class<T> exp, String doc, String path, Operation code, String value, boolean mkdirP)
            throws SubdocException {
        try {
            Executor.execute(doc, path, code, value, mkdirP);
            fail();
        } catch (SubdocException ex) {
            assertTrue(String.format("Expected %s. Got %s", exp.getName(), ex.getClass().getName()), exp.isInstance(ex));
        }
    }

    private static void assertPathNotFound(String doc, String path, Operation code, String value, boolean isMkdir)
            throws SubdocException {
        assertRaisesPriv(PathNotFoundException.class, doc, path, code, value, isMkdir);
    }

    private static void assertPathNotFound(String doc, String path) throws SubdocException {
        assertPathNotFound(doc, path, Operation.GET, null, false);
    }

    private static void assertPathExists(String doc, String path) throws SubdocException {
        assertPathExists(doc, path, null);
    }

    private static void assertPathExists(String doc, String path, String exp) throws SubdocException {
        Result res = Executor.execute(doc, path, Operation.GET);
        if (exp != null) {
            assertEquals(exp, res.getMatch().getAsString());
        }
    }

    private static void assertExistsError(String doc, String path, Operation code, String value) throws SubdocException {
        assertRaisesPriv(PathExistsException.class, doc, path, code, value, false);
    }

    private static void assertMismatchError(String doc, String path, Operation code, String value) throws SubdocException {
        assertMismatchError(doc, path, code, value, false);
    }
    private static void assertMismatchError(String doc, String path, Operation code, String value, boolean isMkdirP) throws SubdocException {
        assertRaisesPriv(PathMismatchException.class, doc, path, code, value, isMkdirP);
    }

    private static void assertCannotInsert(String doc, String path, Operation code, String value) throws SubdocException {
        assertRaisesPriv(CannotInsertException.class, doc, path, code, value, false);
    }

    @Test
    public void testOperations() throws SubdocException {
        String doc = rootText;
        Result res = Executor.execute(doc, "name", Operation.GET);
        assertNotNull(res.getMatch());
        assertEquals("Allagash Brewing", res.getMatch().getAsString());

        res = Executor.execute(doc, "name", Operation.EXISTS);

        res = Executor.execute(doc, "address", Operation.REMOVE);
        assertNotNull(res.getNewDocument());

        doc = res.getNewDocument().toString();
        assertPathNotFound(doc, "address");

        res = Executor.execute(doc, "address", Operation.DICT_ADD, "\"123 Main St.\"");
        doc = res.getNewDocument().toString();

        res = Executor.execute(doc, "address", Operation.GET);
        assertNotNull(res);
        assertEquals("123 Main St.", res.getMatch().getAsString());

        res = Executor.execute(doc, "address", Operation.REPLACE, "\"33 Marginal Rd.\"");
        doc = res.getNewDocument().toString();

        res = Executor.execute(doc, "address", Operation.GET);
        assertEquals("33 Marginal Rd.", res.getMatch().getAsString());

        assertPathNotFound(rootText, "foo.bar.baz", Operation.DICT_ADD, "[1,2,3]", false);

        // Try with P
        res = Executor.execute(doc, "foo.bar.baz", Operation.DICT_ADD, "[1,2,3]", true);
        doc = res.getNewDocument().toString();

        assertEquals("[1,2,3]", Executor.executeGet(doc, "foo.bar.baz").toString());
    }

    @Test
    public void testGenericOps() throws SubdocException {
        String doc = rootText;
        Result res = Executor.execute(doc, "address[0]", Operation.REMOVE);
        JsonElement elem;
        doc = res.getNewDocument().toString();
        assertPathNotFound(doc, "address[0]");

        res = Executor.execute(doc, "address", Operation.REPLACE, "[\"500 B St.\", \"Anytown\", \"USA\"]");
        doc = res.getNewDocument().toString();

        elem = Executor.executeGet(doc, "address[2]");
        assertEquals("USA", elem.getAsString());

        res = Executor.execute(doc, "address[1]", Operation.REPLACE, "\"Sacramento\"");
        doc = res.getNewDocString();
        elem = Executor.executeGet(doc, "address[1]");
        assertEquals("Sacramento", elem.getAsString());
    }

    // TODO: testReplaceArrayDeep

    @Test
    public void testListOps() throws SubdocException {
        String doc = "{}";
        Result res;
        JsonElement elem;

        res = Executor.execute(doc, "array", Operation.DICT_UPSERT, "[]");
        doc = res.getNewDocString();

        res = Executor.execute(doc, "array", Operation.ARRAY_APPEND, "1");
        doc = res.getNewDocString();
        assertPathExists(doc, "array[0]", "1");

        res = Executor.execute(doc, "array", Operation.ARRAY_PREPEND, "0");
        doc = res.getNewDocString();
        assertPathExists(doc, "array[0]", "0");
        assertPathExists(doc, "array[1]", "1");

        res = Executor.execute(doc, "array", Operation.ARRAY_APPEND, "2");
        doc = res.getNewDocString();
        assertPathExists(doc, "array[2]", "2");

        res = Executor.execute(doc, "array", Operation.ARRAY_APPEND, "{\"foo\":\"bar\"}");
        doc = res.getNewDocString();
        elem = Executor.executeGet(doc, "array[3]");
        assertTrue(elem.isJsonObject());

        elem = Executor.executeGet(doc, "array[3].foo");
        assertEquals("bar", elem.getAsString());

        // Test array removal...
        res = Executor.execute(doc, "array[0]", Operation.REMOVE);
        assertEquals("0", res.getMatch().getAsString());
        doc = res.getNewDocString();

        // Should be '1' now
        assertPathExists(doc, "array[0]", "1");

        // "POP"
        res = Executor.execute(doc, "array[-1]", Operation.REMOVE);
        assertTrue(res.getMatch().isJsonObject());
        doc = res.getNewDocString();

        res = Executor.execute(doc, "array[-1]", Operation.REMOVE);
        assertEquals(2, res.getMatch().getAsInt());
    }

    @Test
    public void testListOpsPrepend() throws SubdocException {
        String doc = "{}";
        Result res;

        assertPathNotFound(doc, "array", Operation.ARRAY_PREPEND, "123", false);

        res = Executor.execute(doc, "array", Operation.ARRAY_PREPEND, "123", true);
        doc = res.getNewDocString();
        assertPathExists(doc, "array[0]", "123");

        // Empty the array now
        res = Executor.execute(doc, "array[0]", Operation.REMOVE);
        assertEquals(123, res.getMatch().getAsInt());
        doc = res.getNewDocString();
        assertEquals("{\"array\":[]}", doc);

        // Prepend the first element
        res = Executor.execute(doc, "array", Operation.ARRAY_PREPEND, "123");
        doc = res.getNewDocString();
        assertPathExists(doc, "array[0]", "123");
    }

    @Test
    public void testArrayMultivalue() throws SubdocException {
        String doc = "{\"array\":[4,5,6]}";
        Result res;
        JsonElement elem;

        res = Executor.execute(doc, "array", Operation.ARRAY_PREPEND, "1,2,3");
        doc = res.getNewDocString();
        elem = Executor.executeGet(doc, "array");
        assertEquals("[1,2,3,4,5,6]", elem.toString());

        res = Executor.execute(doc, "array", Operation.ARRAY_APPEND, "7,8,9");
        doc = res.getNewDocString();
        elem = Executor.executeGet(doc, "array");
        assertEquals("[1,2,3,4,5,6,7,8,9]", elem.toString());

        res = Executor.execute(doc, "array[3]", Operation.ARRAY_INSERT, "-3,-2,-1");
        doc = res.getNewDocString();
        elem = Executor.executeGet(doc, "array[4]");
        assertEquals(-2, elem.getAsInt());
    }

    @Test
    public void testArrayOpsNested() throws SubdocException {
        String doc = "[0,[1,[2]],{\"key\":\"val\"}]";
        Result res;

        res = Executor.execute(doc, "[1][1][0]", Operation.REMOVE);
        doc = res.getNewDocString();
        assertEquals("[0,[1,[]],{\"key\":\"val\"}]", doc);

        res = Executor.execute(doc, "[1][1]", Operation.REMOVE);
        doc = res.getNewDocString();
        assertEquals("[0,[1],{\"key\":\"val\"}]", doc);
    }

    @Test
    public void testArrayDelete() throws SubdocException {
        String doc = "[1,2]";
        Result res;

        res = Executor.execute(doc, "[0]", Operation.REMOVE);
        assertEquals("[2]", res.getNewDocString());

        res = Executor.execute(doc, "[1]", Operation.REMOVE);
        assertEquals("[1]", res.getNewDocString());

        doc = "[1]";
        res = Executor.execute(doc, "[0]", Operation.REMOVE);
        assertEquals("[]", res.getNewDocString());

        res = Executor.execute(doc, "[-1]", Operation.REMOVE);
        assertEquals("[]", res.getNewDocString());
    }

    @Test
    public void testDictDelete() throws SubdocException {
        String doc = "{\"0\": 1,\"1\": 2.0}";
        Result res;

        res = Executor.execute(doc, "0", Operation.REMOVE);
        doc = res.getNewDocString();
        assertPathNotFound(doc, "0");
    }

    @Test
    public void testUnique() throws SubdocException {
        String doc = "{}";
        Result res;

        res = Executor.execute(doc, "unique", Operation.ADD_UNIQUE, "\"value\"", true);
        doc = res.getNewDocString();
        assertExistsError(doc, "unique", Operation.ADD_UNIQUE, "\"value\"");

        res = Executor.execute(doc, "unique", Operation.ADD_UNIQUE, "1");
        doc = res.getNewDocString();

        res = Executor.execute(doc, "unique", Operation.ADD_UNIQUE, "\"1\"");
        doc = res.getNewDocString();

        assertCannotInsert(doc, "unique", Operation.ADD_UNIQUE, "[]");
        assertCannotInsert(doc, "unique", Operation.ADD_UNIQUE, "1,2,3");
        // Should succeed
        Executor.execute(doc, "unique", Operation.ADD_UNIQUE, "null");

        // Add a complex object
        res = Executor.execute(doc, "unique", Operation.ARRAY_APPEND, "[]");
        doc = res.getNewDocString();

        assertMismatchError(doc, "unique", Operation.ADD_UNIQUE, "123456");
    }

    @Test
    public void testUniqueTopLevel() throws SubdocException {
        String doc = "[]";
        Result res;

        res = Executor.execute(doc, "", Operation.ADD_UNIQUE, "0");
        doc = res.getNewDocString();

        assertExistsError(doc, "", Operation.ADD_UNIQUE, "0");
    }

    @Test
    public void testNumeric() throws SubdocException {
        String doc = "{}";
        Result res;

        res = Executor.execute(doc, "counter", Operation.COUNTER, "1", true);
        assertEquals(1, res.getMatch().getAsInt());
        doc = res.getNewDocString();

        res = Executor.execute(doc, "counter", Operation.COUNTER, "-101");
        assertEquals(-100, res.getMatch().getAsInt());
        doc = res.getNewDocString();

        res = Executor.execute(doc, "counter", Operation.COUNTER, "1");
        assertEquals(-99, res.getMatch().getAsInt());
        doc = res.getNewDocString();

        // Get it raw
        res = Executor.execute(doc, "counter", Operation.GET);
        assertEquals(-99, res.getMatch().getAsInt());

        // Try with bigger limits
        res = Executor.execute(doc, "counter", Operation.COUNTER, Long.toString(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE-99, res.getMatch().getAsLong());
        doc = res.getNewDocString();

        res = Executor.execute(doc, "counter", Operation.COUNTER, Long.toString(-(Long.MAX_VALUE-99)));
        assertEquals(0, res.getMatch().getAsInt());
        doc = res.getNewDocString();

        // Try with another counter
        res = Executor.execute(doc, "counter2", Operation.DICT_ADD, "9999999999999999999999999999999");
        doc = res.getNewDocString();

        res = Executor.execute(doc, "counter2", Operation.GET);
        assertEquals(new BigInteger("9999999999999999999999999999999"), res.getMatch().getAsBigInteger());

        // Try incrementing a number that's too big..
        assertRaisesPriv(NumberTooBigException.class, doc, "counter2", Operation.COUNTER, "1", false);

        res = Executor.execute(doc, "counter3", Operation.DICT_ADD, "3.14");
        doc = res.getNewDocString();
        assertMismatchError(doc, "counter3", Operation.COUNTER, "1");

        doc = "[]";
        assertPathNotFound(doc, "[0]", Operation.COUNTER, "1", false);
        assertPathNotFound(doc, "[0]", Operation.COUNTER, "1", true);

        res = Executor.execute(doc, "", Operation.ARRAY_APPEND, "-20");
        doc = res.getNewDocString();

        res = Executor.execute(doc, "[0]", Operation.COUNTER, "1");
        assertEquals(-19, res.getMatch().getAsInt());
    }

    @Test
    public void testBadNumFormat() throws SubdocException {
        String doc = "{}";
        assertRaisesPriv(BadNumberException.class, doc, "pth", Operation.COUNTER, "bad", false);
        assertRaisesPriv(BadNumberException.class, doc, "pth", Operation.COUNTER, "3.14", false);
        assertRaisesPriv(BadNumberException.class, doc, "pth", Operation.COUNTER, "-", false);
        assertRaisesPriv(BadNumberException.class, doc, "pth", Operation.COUNTER, "43f", false);
        assertRaisesPriv(ZeroDeltaException.class, doc, "pth", Operation.COUNTER, "0", false);
    }

    @Test
    public void testNumericLimits() throws SubdocException {
        String doc = "{\"counter\":" + Long.toString(Long.MAX_VALUE-1) + "}";
        Result res;

        res = Executor.execute(doc, "counter", Operation.COUNTER, "1");
        assertEquals(Long.MAX_VALUE, res.getMatch().getAsLong());
        doc = res.getNewDocString();

        assertRaisesPriv(DeltaTooBigException.class, doc, "counter", Operation.COUNTER, "2", false);

        doc = "{\"counter\":" + Long.toString(Long.MIN_VALUE+1) + "}";
        res = Executor.execute(doc, "counter", Operation.COUNTER, "-1");
        assertEquals(Long.MIN_VALUE, res.getMatch().getAsLong());
        doc = res.getNewDocString();
        assertRaisesPriv(DeltaTooBigException.class, doc, "counter", Operation.COUNTER, "-2", false);
    }

    private static void assertBadDictValue(String value) throws SubdocException{
        assertRaisesPriv(CannotInsertException.class, "{}", "a_path", Operation.DICT_ADD, value, true);
        assertRaisesPriv(CannotInsertException.class, "{}", "a_path", Operation.DICT_ADD, value, false);
    }

    @Test
    public void testValueValidation() throws SubdocException {
        // Gson seems to accept unquoted keys and values!
        assertBadDictValue("INVALID");
        assertBadDictValue("1,2,3,4");
        assertBadDictValue("1,\"k2\":2");
        assertBadDictValue("{ \"foo\" }");
        assertBadDictValue("{ \"foo\": }");
        assertBadDictValue("nul");
        assertBadDictValue("2.0.0");
        assertBadDictValue("2.");
        assertBadDictValue("2.0e");
        assertBadDictValue("2.0e+");
    }

    @Test
    public void testNegativeIndex() throws SubdocException {
        String doc = "[1,2,3,4,5,6]";
        Result res;
        JsonElement elem;

        elem = Executor.executeGet(doc, "[-1]");
        assertEquals(6, elem.getAsInt());

        doc = "[1,2,3,[4,5,6,[7,8,9]]]";
        elem = Executor.executeGet(doc, "[-1][-1][-1]");
        assertEquals(9, elem.getAsInt());

        res = Executor.execute(doc, "[-1][-1][-1]", Operation.REMOVE);
        doc = res.getNewDocString();

        // Push new value
        res = Executor.execute(doc, "[-1][-1]", Operation.ARRAY_APPEND, "10");
        doc = res.getNewDocString();
        assertPathExists(doc, "[-1][-1][-1]", "10");

        // Intermixed paths
        doc = "{\"k1\": [\"first\", {\"k2\":[6,7,8]},\"last\"] }";
        elem = Executor.executeGet(doc, "k1[-1]");
        assertEquals("last", elem.getAsString());

        elem = Executor.executeGet(doc, "k1[1].k2[-1]");
        assertEquals(8, elem.getAsInt());
    }

    @Test
    public void testRootOps() throws SubdocException {
        String doc = "[]";
        Result res;
        JsonElement elem;

        elem = Executor.executeGet(doc, "");
        assertEquals("[]", elem.toString());

        res = Executor.execute(doc, "", Operation.ARRAY_APPEND, "null");
        doc = res.getNewDocString();
        elem = Executor.executeGet(doc, "[0]");
        assertTrue(elem.isJsonNull());

        assertRaisesPriv(CannotInsertException.class, doc, "", Operation.REMOVE, null, false);
    }

    @Test
    public void testMismatch() throws SubdocException {
        Result res;
        JsonElement elem;

        assertMismatchError("{}", "", Operation.ARRAY_APPEND, "null");
        assertCannotInsert("[]", "", Operation.DICT_UPSERT, "blah");
        assertCannotInsert("[]", "key", Operation.DICT_UPSERT, "blah");

        String doc = "[null]";
        assertCannotInsert(doc, "", Operation.DICT_UPSERT, "blah");
        assertCannotInsert(doc, "key", Operation.DICT_UPSERT, "blah");
        assertMismatchError(doc, "foo.bar", Operation.ARRAY_APPEND, "null", true);
    }

    @Test
    public void testWhitespace() throws SubdocException {
        // This test might be more relevant for the C Json parser itself
        String doc = "[ 1, 2, 3,       4        ]";
        assertPathExists(doc, "[-1]", "4");
    }


    /*
    // Following disabled because we don't do depth verification
    @Test
    public void testTooDeep() throws SubdocException {
    }
    @Test
    public void testTooDeepDict() throws SubdocException {
    }
    */

    @Test
    public void testArrayInsert() throws SubdocException {
        String doc = "[1,2,4,5]";
        Result res;
        JsonElement elem;

        res = Executor.execute(doc, "[2]", Operation.ARRAY_INSERT, "3");
        doc = res.getNewDocString();
        assertPathExists(doc, "[2]", "3");
        assertEquals(5, res.getNewDocument().getAsJsonArray().size());

        // Effective prepend via insert
        res = Executor.execute(doc, "[0]", Operation.ARRAY_INSERT, "0");
        doc = res.getNewDocString();
        assertPathExists(doc, "[0]", "0");

        // Negative index for INSERT not allowed
        doc = "[1,2,3,5]";
        assertRaisesPriv(InvalidPathException.class, doc, "[-1]", Operation.ARRAY_INSERT, "4", false);

        // Test out of bounds
        doc = "[1,2,3]";
        assertPathNotFound(doc, "[4]", Operation.ARRAY_INSERT, "null", false);

        // Not using array syntax (final path component is not an array)
        assertMismatchError(doc, "[0].anything", Operation.ARRAY_INSERT, "null");

        // Try with missing parent. Should fail
        doc = "{}";
        assertPathNotFound(doc, "non_exist[0]", Operation.ARRAY_INSERT, "null", false);

        doc = "[]";
        assertCannotInsert(doc, "[0]", Operation.ARRAY_INSERT, "blah");

        doc = "{}";
        assertPathInvalidError(doc, "[0]", Operation.ARRAY_INSERT, "null");
    }

    private void assertPathInvalidError(String doc, String path, Operation code, String value)
            throws SubdocException {
        assertRaisesPriv(PathParseException.class, doc, path, code, value, false);
    }

    private void assertEmptyError(Operation op, String path) throws SubdocException {
        assertRaisesPriv(EmptyValueException.class, "{}", path, op, null, false);
    }

    @Test
    public void testEmpty() throws SubdocException {
        assertEmptyError(Operation.DICT_ADD, "p");
        assertEmptyError(Operation.DICT_UPSERT, "p");
        assertEmptyError(Operation.REPLACE, "p");
        assertEmptyError(Operation.ARRAY_APPEND, "p");
        assertEmptyError(Operation.ARRAY_PREPEND, "p");
        assertEmptyError(Operation.ADD_UNIQUE, "p");
        assertEmptyError(Operation.ARRAY_INSERT, "p[0]");
    }

    @Test
    public void testDeleteNestedArray() throws SubdocException {
        String doc = "[0,[10,20,[100]],{\"key\":\"value\"}]";
        Result res;
        JsonElement elem;

        elem = Executor.executeGet(doc, "[1]");
        assertEquals("[10,20,[100]]", elem.toString());

        res = Executor.execute(doc, "[1][2][0]", Operation.REMOVE);
        doc = res.getNewDocString();

        elem = Executor.executeGet(doc, "[1]");
        assertEquals("[10,20,[]]", elem.toString());

        res = Executor.execute(doc, "[1][2]", Operation.REMOVE);
        doc = res.getNewDocString();

        elem = Executor.executeGet(doc, "[1]");
        assertEquals("[10,20]", elem.toString());

        res = Executor.execute(doc, "[1]", Operation.REMOVE);
        doc = res.getNewDocString();

        elem = Executor.executeGet(doc, "[1]");
        assertEquals("{\"key\":\"value\"}", elem.toString());
    }

    @Test
    public void testGetCount() throws SubdocException {
        String doc = "{}";
        JsonElement elem;

        elem = Executor.execute(doc, "", Operation.GET_COUNT).getMatch();
        assertEquals(0, elem.getAsInt());

        doc = "[]";
        elem = Executor.execute(doc, "", Operation.GET_COUNT).getMatch();
        assertEquals(0, elem.getAsInt());

        doc = "{\"hello\": \"world\"}";
        elem = Executor.execute(doc, "", Operation.GET_COUNT).getMatch();
        assertEquals(1, elem.getAsInt());
        assertEquals("1", elem.toString());

        assertRaisesPriv(PathMismatchException.class, doc, "hello", Operation.GET_COUNT, null, false);
        assertRaisesPriv(PathNotFoundException.class, doc, "nonexist", Operation.GET_COUNT, null, false);
    }
}