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
import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MatchTest {
    @Test
    public void testSimpleDictMatch() throws Exception {
        String doc = "{\"key\":\"value\"}";
        Match m = Match.match(doc, "key");

        assertTrue(m.isFound());
        assertEquals(m.getDeepest(), m.getMatch());
        JsonElement deepest = m.getDeepest();
        assertTrue(deepest.isJsonPrimitive());
        assertEquals("value", deepest.getAsString());

        JsonElement parent = m.getMatchParent();
        assertTrue(parent.isJsonObject());
        assertEquals(m.getImmediateParent(), m.getMatchParent());
    }

    @Test
    public void testSimpleArrayMatch() throws Exception {
        String doc = "{\"key\":{\"array\":[1,2,3]}}";
        Match m = Match.match(doc, "key.array[0]");
        assertTrue(m.isFound());

        JsonElement deepest = m.getDeepest();
        assertTrue(deepest.isJsonPrimitive());
        assertEquals(1, deepest.getAsInt());

        JsonElement parent = m.getMatchParent();
        assertTrue(parent.isJsonArray());
    }

    @Test
    public void testMissingParents() throws Exception {
        String doc = "{\"foo\":1, \"bar\":2, \"baz\": 3}";
        Match m = Match.match(doc, "nonexist");
        assertFalse(m.isFound());
        assertTrue(m.hasImmediateParent());

        JsonElement parent = m.getImmediateParent();
        assertTrue(parent.isJsonObject());
        assertEquals(3, parent.getAsJsonObject().entrySet().size());

        // Test a deeper path
        doc = "{\"level1\":{\"level2\":{\"level3\":{\"sibling\":\"element\"}}}}";
        m = Match.match(doc, "level1.level2.level3.level4");
        assertFalse(m.isFound());
        parent = m.getImmediateParent();
        assertTrue(parent.isJsonObject());
        JsonObject object = parent.getAsJsonObject();
        assertNotNull(object.get("sibling"));
    }

    @Test(expected = PathMismatchException.class)
    public void testMismatch1() throws Exception {
        String doc = "{\"foo\":\"bar\"}";
        Match.match(doc, "foo.bar.baz");
    }

    @Test(expected = PathMismatchException.class)
    public void testMismatch2() throws Exception {
        String doc = "{\"foo\":[]}";
        Match.match(doc, "foo.bar");
    }


    @Test(expected = PathMismatchException.class)
    public void testMismatch3() throws Exception {
        String doc = "{\"foo\":{}}";
        Match.match(doc, "foo[0]");
    }
}