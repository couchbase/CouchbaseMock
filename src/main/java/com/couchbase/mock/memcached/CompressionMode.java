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

package com.couchbase.mock.memcached;

public enum CompressionMode {
    OFF("off"),
    PASSIVE("passive"),
    ACTIVE("active");

    private final String value;

    CompressionMode(String value) {
        this.value = value;
    }

    public static CompressionMode of(String value) {
        for (CompressionMode mode : CompressionMode.values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("No enum constant for \"" + value + "\"");
    }

    public String value() {
        return value;
    }
}
