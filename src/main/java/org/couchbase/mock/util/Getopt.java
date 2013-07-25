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

import java.util.ArrayList;
import java.util.List;

/**
 * Small class to ease command line parsing.
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 * @version 1.0
 */
public class Getopt {

    /**
     * Create a new instance of the option parser
     */
    public Getopt() {
        options = new ArrayList<CommandLineOption>();
        optind = -1;
    }

    /**
     * Add a new option to the list of options we accept
     *
     * @param option the new option
     * @return ourselves so that you may chain the calls
     */
    public Getopt addOption(CommandLineOption option) {
        options.add(option);
        return this;
    }

    /**
     * Parse the given hasArgument vector
     *
     * @param argv The arguments to parse
     * @return The user-specified options
     */
    public List<Entry> parse(String[] argv) {
        optind = -1;

        List<Entry> ret = new ArrayList<Entry>();

        int idx = 0;
        while (idx < argv.length) {
            if (argv[idx].equals("--")) {
                // End of options!
                ++idx;
                break;
            }

            if (argv[idx].charAt(0) != '-') {
                // End of options
                break;
            }


            if (argv[idx].startsWith("--")) {
                idx = parseLongOption(argv, ret, idx);
            } else if (argv[idx].startsWith("-")) {
                idx = parseShortOption(argv, ret, idx);
            } else {
                break;
            }
            ++idx;
        }

        if (idx != argv.length) {
            optind = idx;
        }

        return ret;
    }

    private int parseShortOption(String[] argv, List<Entry> ret, int idx) {
        String keys = argv[idx].substring(1);
        for (char c : keys.toCharArray()) {
            String key = "-" + c;
            boolean found = false;
            for (CommandLineOption o : options) {
                if (key.charAt(1) == o.shortOption) {
                    found = true;
                    // this is a match :)
                    String value = null;
                    if (o.hasArgument) {
                        if (idx + 1 < argv.length) {
                            value = argv[idx + 1];
                            ++idx;
                        } else {
                            throw new IllegalArgumentException("option requires an argument -- " + key);
                        }
                    }
                    ret.add(new Entry(key, value));
                }
            }

            if (!found) {
                // Illegal option!!!!!
                throw new IllegalArgumentException("Illegal option -- " + key);
            }
        }
        return idx;
    }

    private int parseLongOption(String[] argv, List<Entry> ret, int idx) {
        String key = argv[idx];
        int ii = key.indexOf('=');
        if (ii != -1) {
            key = key.substring(0, ii);
        }

        // Try to look up the option
        boolean found = false;
        for (CommandLineOption o : options) {
            if (key.equals(o.longOption)) {
                found = true;
                // this is a match :)
                String value = null;
                if (o.hasArgument) {
                    if (ii != -1) {
                        value = argv[idx].substring(ii + 1);
                    } else if (idx + 1 < argv.length) {
                        value = argv[idx + 1];
                        ++idx;
                    } else {
                        throw new IllegalArgumentException("option requires an argument -- " + key);
                    }
                }
                ret.add(new Entry(key, value));
            }
        }

        if (!found) {
            // Illegal option!!!!!
            throw new IllegalArgumentException("Illegal option -- " + key);
        }
        return idx;
    }

    /**
     * Get the index of the first non-argument option.
     *
     * @return The position in the argument vector for the first non-argument option
     */
    public int getOptind() {
        return optind;
    }

    private int optind;
    private final List<CommandLineOption> options;

    /**
     * The command line options must be specified with a short and a long option
     * (note that you may specify null for the short option and the long option
     * to disable it).
     */
    public static class CommandLineOption {

        private final char shortOption;
        private final String longOption;
        private final boolean hasArgument;

        /**
         * Create a new instance of the command line option
         *
         * @param shortOption    the single character for the option
         * @param longOption     the name of the long option
         * @param hasArgument if this option takes a mandatory argument
         */
        public CommandLineOption(char shortOption, String longOption, boolean hasArgument) {
            this.shortOption = shortOption;
            this.longOption = longOption;
            this.hasArgument = hasArgument;
        }
    }

    public static class Entry {

        public final String key;
        public final String value;

        public Entry(String k, String v) {
            key = k;
            value = v;
        }
    }
}
