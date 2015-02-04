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

package org.couchbase.mock.memcached.protocol;

import org.couchbase.mock.memcached.VBucketCoordinates;

/**
 * Created by mnunberg on 2/4/15.
 */
public class BinaryObserveSeqnoResponse extends BinaryResponse {
    // Base is:
    // Format   (1)  = 1
    // VBID     (2)  = 3
    // UUID     (8)  = 11
    // SEQ(C)   (8)  = 19
    // SEQ(D)   (8)  = 27

    static final int REPLY_LENGTH_NORMAL = 27;
    static final int REPLY_LENGTH_FAILOVER = REPLY_LENGTH_NORMAL + 16;
    static final int COMMON_OFFSET = 25;

    private void writeInfoCommon(short vbid, long uuid, long seqCache, long seqDisk) {
        buffer.position(COMMON_OFFSET);
        buffer.putShort(vbid);
        buffer.putLong(uuid);
        buffer.putLong(seqDisk);
        buffer.putLong(seqCache);
    }

    /**
     * Construct a "Normal" response
     * @param cmd The command
     * @param seqCache The sequence number for the cache
     * @param seqDisk The sequence number for persistence
     */
    public BinaryObserveSeqnoResponse(BinaryObserveSeqnoCommand cmd, long seqCache, long seqDisk) {
        super(cmd, ErrorCode.SUCCESS, 0, 0, REPLY_LENGTH_NORMAL, 0);
        buffer.put(24, (byte) 0x00);
        writeInfoCommon(cmd.getVBucketId(), cmd.getUuid(), seqCache, seqDisk);
        buffer.rewind();
    }

    /**
     * Construct a "Failover" response. This is issued when a valid but old UUID is
     * passed in the request.
     * @param cmd The command
     * @param coordCur The current coordinates of the vBucket
     * @param coordOld The old coordinates of the vBucket corresponding to the request
     * @param seqDisk The sequence number (for persistence) for the current coordinates.
     */
    public BinaryObserveSeqnoResponse(
            BinaryObserveSeqnoCommand cmd, VBucketCoordinates coordCur, VBucketCoordinates coordOld, long seqDisk) {

        super(cmd, ErrorCode.SUCCESS, 0, 0, REPLY_LENGTH_FAILOVER, 0);
        buffer.put(24, (byte)0x01);

        writeInfoCommon(cmd.getVBucketId(), coordCur.getUuid(), coordCur.getSeqno(), seqDisk);
        buffer.putLong(coordOld.getUuid());
        buffer.putLong(coordOld.getSeqno());
        buffer.rewind();
    }
}
