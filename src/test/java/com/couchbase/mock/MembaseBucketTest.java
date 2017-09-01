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
package com.couchbase.mock;

import com.couchbase.mock.Bucket.BucketType;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * Just a small test case to call the JSON generation..
 *
 * @author Trond
 */
public class MembaseBucketTest extends TestCase {

    public MembaseBucketTest(String testName) {
        super(testName);
    }

    /**
     * Test of getJSON method, of class CouchbaseBucket.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void testGetJSON() throws IOException {
        BucketConfiguration config = new BucketConfiguration();
        config.type = BucketType.COUCHBASE;
        config.name = "membase";
        CouchbaseMock mock = new CouchbaseMock("localhost", 8091, 1, 1024);
        CouchbaseBucket instance = new CouchbaseBucket(mock, config);
        assertNotNull(instance.getJSON());
    }
}
