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

/**
 * Path component
 */
class Component {
    final private String value;
    final private int numval;
    private final static int STRING_VALUE = -2;

    Component(String input, boolean isIndex) throws PathParseException {
        if (input == null || input.isEmpty()) {
            throw new PathParseException("Component cannot be empty");
        }
        value = input;
        if (isIndex) {
            try {
                numval = Integer.parseInt(input);
            } catch (NumberFormatException e) {
                throw new PathParseException(e);
            }
            if (numval < -1) {
                throw new PathParseException("Negative path other than -1 not allowed");
            }
        } else {
            numval = STRING_VALUE;
        }
    }

    Component() {
        value = null;
        numval = STRING_VALUE;
    }

    int getIndex() {
        return numval;
    }

    String getString() {
        return value;
    }

    boolean isIndex() {
        return numval != STRING_VALUE;
    }
}
