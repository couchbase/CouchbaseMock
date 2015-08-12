/*
 * Copyright 2011 Couchbase, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The Info class is used to have somewhere to store basic information
 * about the Couchbase Mock
 */
public final class Info {
    private static final String VERSION = "1.1.0";

    /**
     * get major version
     * @return major version
     */
    public static String getVersion() {
        return VERSION;
    }

    /**
     * get full version (product vMajor revMinor)
     * @return full version
     */
    public static String getFullVersion() {
        return "CouchbaseMock v" + VERSION;
    }

    private static final AtomicLong clockOffset = new AtomicLong();

    public static void timeTravel(long offset) {
        clockOffset.addAndGet(offset);
    }

    public static long getClockOffset() {
        return clockOffset.get();
    }


    private Info() {
    }
}
