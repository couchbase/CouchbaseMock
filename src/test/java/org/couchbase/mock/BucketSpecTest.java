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

    public void testDefaults() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, 100, 4096);
        Map<String, Bucket> buckets = mock.getBuckets();
        assertEquals(1, buckets.size());
        assert(buckets.containsKey("default"));
        assertEquals("", buckets.get("default").getPassword());
        assertEquals(CouchbaseBucket.class, buckets.get("default").getClass());
    }

    public void testPasswords() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, 100, 4096, "xxx:,yyy:pass,zzz");
        Map<String, Bucket> buckets = mock.getBuckets();
        assertEquals(3, buckets.size());
        assert(buckets.containsKey("xxx"));
        assert(buckets.containsKey("yyy"));
        assert(buckets.containsKey("zzz"));
        assertEquals("", buckets.get("xxx").getPassword());
        assertEquals("", buckets.get("zzz").getPassword());
        assertEquals("pass", buckets.get("yyy").getPassword());
    }

    public void testTypes() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, 100, 4096, "xxx::,yyy::memcache,zzz,kkk::couchbase,aaa::unknown");
        Map<String, Bucket> buckets = mock.getBuckets();
        assertEquals(5, buckets.size());
        assert(buckets.containsKey("xxx"));
        assert(buckets.containsKey("yyy"));
        assert(buckets.containsKey("zzz"));
        assert(buckets.containsKey("kkk"));
        assert(buckets.containsKey("aaa"));
        assertEquals(CouchbaseBucket.class, buckets.get("xxx").getClass());
        assertEquals(MemcachedBucket.class, buckets.get("yyy").getClass());
        assertEquals(CouchbaseBucket.class, buckets.get("zzz").getClass());
        assertEquals(CouchbaseBucket.class, buckets.get("kkk").getClass());
        assertEquals(CouchbaseBucket.class, buckets.get("aaa").getClass());
    }

    public void testMixed() throws Exception {
        CouchbaseMock mock = new CouchbaseMock(null, 8091, 100, 4096, "xxx:pass:memcache,yyy:secret:couchbase");
        Map<String, Bucket> buckets = mock.getBuckets();
        assertEquals(2, buckets.size());
        assert(buckets.containsKey("xxx"));
        assert(buckets.containsKey("yyy"));
        assertEquals(MemcachedBucket.class, buckets.get("xxx").getClass());
        assertEquals(CouchbaseBucket.class, buckets.get("yyy").getClass());
        assertEquals("pass", buckets.get("xxx").getPassword());
        assertEquals("secret", buckets.get("yyy").getPassword());
    }


}
