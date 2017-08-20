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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PathTest {

    private void assertStringComponent(Path p, int pos, String value) {
        Component comp = p.get(pos);
        assertFalse(comp.isIndex());
        assertEquals(value, comp.getString());
    }

    private void assertIndexComponent(Path p, int pos, int value) {
        Component comp = p.get(pos);
        assertTrue(comp.isIndex());
        assertEquals(value, comp.getIndex());
    }

    @Test
    public void testRoot() throws Exception {
        Path p = new Path("");
        assertEquals(0, p.size());
    }

    @Test
    public void testSingle() throws Exception {
        Path p = new Path("hello");
        assertEquals(1, p.size());
        assertEquals("hello", p.get(0).getString());
        assertFalse(p.get(0).isIndex());
    }

    @Test
    public void testMultiObject() throws Exception {
        Path p = new Path("foo.bar.baz");
        assertEquals(3, p.size());

        assertStringComponent(p, 0, "foo");
        assertStringComponent(p, 1, "bar");
        assertStringComponent(p, 2, "baz");
    }

    @Test
    public void testBackticks() throws Exception {
        Path p = new Path("`foo`");
        assertEquals(1, p.size());
        assertStringComponent(p, 0, "foo");

        p = new Path("`foo.bar`");
        assertEquals(1, p.size());
        assertStringComponent(p, 0, "foo.bar");

        p = new Path("`foo.bar`.baz");
        assertEquals(2, p.size());
        assertStringComponent(p, 0, "foo.bar");
        assertStringComponent(p, 1, "baz");

        p = new Path("back``tick.foo.bar");
        assertEquals(3, p.size());
        assertStringComponent(p, 0, "back`tick");
        assertStringComponent(p, 1, "foo");
        assertStringComponent(p, 2, "bar");
    }

    @Test
    public void testArrayIndex() throws Exception {
        Path p = new Path("[0]");
        assertEquals(1, p.size());
        assertIndexComponent(p, 0, 0);

        // Test mixed indexes
        p = new Path("[0][1][2]");
        assertEquals(3, p.size());

        for (int i = 0; i < 3; i++) {
            assertIndexComponent(p, i, i);
        }

        // Test mixed indexes
        p = new Path("foo.bar.baz[0].blah");
        assertEquals(5, p.size());
        assertStringComponent(p, 0, "foo");
        assertStringComponent(p, 1, "bar");
        assertStringComponent(p, 2, "baz");
        assertIndexComponent(p, 3, 0);
        assertStringComponent(p, 4, "blah");

        p = new Path("foo[0]");
        assertEquals(2, p.size());
        assertStringComponent(p, 0, "foo");
        assertIndexComponent(p, 1, 0);
    }

    @Test(expected = PathParseException.class)
    public void testEmptyLastPath() throws Exception{
        new Path("foo.");
    }

    @Test(expected = PathParseException.class)
    public void testUnclosedBracket1() throws Exception {
        new Path("[");
    }

    @Test(expected = PathParseException.class)
    public void testUnclosedBracket2() throws Exception {
        new Path("[0");
    }

    @Test(expected = PathParseException.class)
    public void testUnopenedBracket() throws Exception {
        new Path("foo.bar.baz2]");
    }

    @Test(expected = PathParseException.class)
    public void testEmptyComponent1() throws Exception {
        new Path("foo..bar");
    }

    @Test(expected = PathParseException.class)
    public void testEmptyComponent2() throws Exception {
        new Path("foo..");
    }

    @Test(expected = PathParseException.class)
    public void testMalformedNumber() throws Exception {
        new Path("[bad_number]");
    }

    @Test(expected = PathParseException.class)
    public void testIllegalNegativeIndex() throws Exception {
        new Path("[-2]");
    }
}