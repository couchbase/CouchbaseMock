/**
 *     Copyright 2011 Membase, Inc.
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

package org.couchbase.mock;

import java.util.Map;
import junit.framework.TestCase;

/**
 * Tests for bucket spec parser
 *
 * @author Sergey Avseyev <sergey.avseyev@gmail.com>
 */
public class BucketSpecTest extends TestCase {
    private int numNodes = 100;
    private int numVBuckets = 4096;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final String platform = System.getProperty("os.name");
        if (platform.equals("Mac OS X") || platform.equals("Linux")) {
            numNodes = 4;
            numVBuckets = 16;
        }
    }

    public void testDefaults() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets);
        Map<String, BucketConfiguration> buckets = mock.getInitialConfigs();
        assertEquals(1, buckets.size());
        assert(buckets.containsKey("default"));
        assertEquals("", buckets.get("default").getPassword());
        assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("default").getType());
    }

    public void testPasswords() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets, "xxx:,yyy:pass,zzz");
        Map<String, BucketConfiguration> buckets = mock.getInitialConfigs();
        assertEquals(3, buckets.size());
        assert(buckets.containsKey("xxx"));
        assert(buckets.containsKey("yyy"));
        assert(buckets.containsKey("zzz"));
        assertEquals("", buckets.get("xxx").getPassword());
        assertEquals("", buckets.get("zzz").getPassword());
        assertEquals("pass", buckets.get("yyy").getPassword());
    }

    public void testTypes() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets, "xxx::,yyy::memcache,zzz,kkk::couchbase,aaa::unknown");
        Map<String, BucketConfiguration> buckets = mock.getInitialConfigs();
        assertEquals(5, buckets.size());
        assert(buckets.containsKey("xxx"));
        assert(buckets.containsKey("yyy"));
        assert(buckets.containsKey("zzz"));
        assert(buckets.containsKey("kkk"));
        assert(buckets.containsKey("aaa"));
        assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("xxx").getType());
        assertEquals(Bucket.BucketType.MEMCACHED, buckets.get("yyy").getType());
        assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("zzz").getType());
        assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("kkk").getType());
        assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("aaa").getType());
    }

    public void testMixed() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, numNodes, numVBuckets, "xxx:pass:memcache,yyy:secret:couchbase");
        Map<String, BucketConfiguration> buckets = mock.getInitialConfigs();
        assertEquals(2, buckets.size());
        assert(buckets.containsKey("xxx"));
        assert(buckets.containsKey("yyy"));
        assertEquals(Bucket.BucketType.MEMCACHED, buckets.get("xxx").getType());
        assertEquals(Bucket.BucketType.COUCHBASE, buckets.get("yyy").getType());
        assertEquals("pass", buckets.get("xxx").getPassword());
        assertEquals("secret", buckets.get("yyy").getPassword());
    }


}
