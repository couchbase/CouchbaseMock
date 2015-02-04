/*
 * Copyright 2015 Couchbase, Inc.
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

package org.couchbase.mock.memcached;

import java.util.concurrent.atomic.AtomicLong;

public class StorageVBucketCoordinates implements VBucketCoordinates {
    private AtomicLong seqno;
    private final long uuid;

    @Override
    public long getSeqno() {
        return seqno.get();
    }

    public long incrSeqno() {
        return seqno.incrementAndGet();
    }

    @Override
    public long getUuid() {
        return uuid;
    }

    void seekSeqno(long at) {
        seqno.set(at);
    }

    public StorageVBucketCoordinates(long id) {
        seqno = new AtomicLong(1);
        uuid = id;
    }

    public StorageVBucketCoordinates(VBucketCoordinates other) {
        seqno = new AtomicLong(other.getSeqno());
        uuid = other.getUuid();
    }
}
