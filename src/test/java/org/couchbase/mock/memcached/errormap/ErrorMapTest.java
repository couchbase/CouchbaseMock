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

package org.couchbase.mock.memcached.errormap;

import junit.framework.TestCase;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.util.ReaderUtils;

/**
 * Created by mnunberg on 4/12/17.
 */
public class ErrorMapTest extends TestCase {
    private final static String ERRMAP_TXT;
    static {
        try {
            ERRMAP_TXT = ReaderUtils.fromResource("errmap/errmap_v1.json");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void testParse() throws Exception {
        // Get the actual error map
        ErrorMap mm = ErrorMap.parse(ERRMAP_TXT);

        // Get an error code:
        ErrorMapEntry entry = mm.getErrorEntry(ErrorCode.KEY_ENOENT.value());
        assertNotNull(entry);
        assertNotNull(entry.getAttrs());
        assertFalse(entry.getAttrs().isEmpty());
        assertNotNull(entry.getName());
        assertNotNull(entry.getDescription());
    }
}