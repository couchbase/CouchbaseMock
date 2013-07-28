/**
 *     Copyright 2011 Couchbase, Inc.
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

import java.util.List;
import junit.framework.TestCase;
import org.couchbase.mock.util.Getopt.CommandLineOption;
import org.couchbase.mock.util.Getopt.Entry;

/**
 * Test the command line parser
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 * @version 1.0
 */
public class GetoptTest extends TestCase {

    private final Getopt getopt;

    public GetoptTest(String testName) {
        super(testName);
        getopt = new Getopt();
        getopt.addOption(new CommandLineOption('a', "--alpha", true)).
                addOption(new CommandLineOption('b', "--bravo", false)).
                addOption(new CommandLineOption('c', "--charlie", false));
    }

    public void testParseEmptyNoOptions() {
        String[] argv = new String[0];
        Getopt instance = new Getopt();
        if (!instance.parse(argv).isEmpty()) {
            fail("Parse should return an empty set");
        }
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseEmpty() {
        String[] argv = new String[0];
        if (!getopt.parse(argv).isEmpty()) {
            fail("Parse should return an empty set");
        }
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseOnlyArguments() {
        String[] argv = {"foo", "bar"};
        if (!getopt.parse(argv).isEmpty()) {
            fail("Parse should return an empty set");
        }
        assertEquals(0, getopt.getOptind());
    }

    public void testParseOnlyArgumentsWithSeparatorInThere() {
        String[] argv = {"foo", "--", "bar"};
        if (!getopt.parse(argv).isEmpty()) {
            fail("Parse should return an empty set");
        }
        assertEquals(0, getopt.getOptind());
    }

    public void testParseSingleLongOptionWithoutArgument() {
        String[] argv = {"--bravo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("--bravo", options.get(0).key);
        assertNull(options.get(0).value);
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseSingleLongOptionWithoutRequiredArgument() {
        String[] argv = {"--alpha"};
        try {
            getopt.parse(argv);
            fail("Parse should throw an exception");
        } catch (IllegalArgumentException exp) {
        }
    }

    public void testParseSingleLongOptionWithRequiredArgument() {
        String[] argv = {"--alpha=foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("--alpha", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseSingleLongOptionWithRequiredArgument1() {
        String[] argv = {"--alpha", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("--alpha", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseMultipleLongOptionWithArgumentsAndOptions() {
        String[] argv = {"--alpha=foo", "--bravo", "--charlie", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(3, options.size());
        assertEquals("--alpha", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals("--bravo", options.get(1).key);
        assertNull(options.get(1).value);
        assertEquals("--charlie", options.get(2).key);
        assertNull(options.get(2).value);
        assertEquals(3, getopt.getOptind());
    }

    public void testParseMultipleLongOptionWithArgumentsAndOptionsAndSeparator() {
        String[] argv = {"--alpha=foo", "--", "--bravo", "--charlie", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("--alpha", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals(2, getopt.getOptind());
    }

    public void testParseMultipleLongOptionWithArgumentsAndOptionsAndSeparator1() {
        String[] argv = {"--alpha", "foo", "--", "--bravo", "--charlie", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("--alpha", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals(3, getopt.getOptind());
    }

    public void testParseSingleShortOptionWithoutArgument() {
        String[] argv = {"-b"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("-b", options.get(0).key);
        assertNull(options.get(0).value);
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseSingleShortOptionWithoutRequiredArgument() {
        String[] argv = {"-a"};
        try {
            getopt.parse(argv);
            fail("Parse should throw an exception");
        } catch (IllegalArgumentException exp) {
        }
    }

    public void testParseSingleShortOptionWithRequiredArgument() {
        String[] argv = {"-a", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("-a", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals(-1, getopt.getOptind());
    }

    public void testParseMultipleShortOptionWithArgumentsAndOptions() {
        String[] argv = {"-a", "foo", "-b", "-c", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(3, options.size());
        assertEquals("-a", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals("-b", options.get(1).key);
        assertNull(options.get(1).value);
        assertEquals("-c", options.get(2).key);
        assertNull(options.get(2).value);
        assertEquals(4, getopt.getOptind());
    }

    public void testParseMultipleShortOptionWithArgumentsAndOptions1() {
        String[] argv = {"-abc", "foo", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(3, options.size());
        assertEquals("-a", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals("-b", options.get(1).key);
        assertNull(options.get(1).value);
        assertEquals("-c", options.get(2).key);
        assertNull(options.get(2).value);
        assertEquals(2, getopt.getOptind());
    }

    public void testParseMultipleShortOptionWithArgumentsAndOptionsAndSeparator() {
        String[] argv = {"-a", "foo", "--", "-b", "-c", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(1, options.size());
        assertEquals("-a", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals(3, getopt.getOptind());
    }

    public void testParseMix() {
        String[] argv = {"--alpha", "foo", "-a", "bar", "-b", "-c", "--bravo", "-bc", "foo"};
        List<Entry> options = getopt.parse(argv);
        assertEquals(7, options.size());
        assertEquals("--alpha", options.get(0).key);
        assertEquals("foo", options.get(0).value);
        assertEquals("-a", options.get(1).key);
        assertEquals("bar", options.get(1).value);
        assertEquals("-b", options.get(2).key);
        assertNull(options.get(2).value);
        assertEquals("-c", options.get(3).key);
        assertNull(options.get(3).value);
        assertEquals("--bravo", options.get(4).key);
        assertNull(options.get(4).value);
        assertEquals("-b", options.get(5).key);
        assertNull(options.get(5).value);
        assertEquals("-c", options.get(6).key);
        assertNull(options.get(6).value);
        assertEquals(8, getopt.getOptind());
    }
}
