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

import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * @author Mark Nunberg
 *
 * This class encapsulates the coordinates and status of a mutation operation.
 * It is intended to be used as a simple container type for the command executors.
 */
public class MutationStatus {
    private final ErrorCode ec;
    private final VBucketCoordinates vbCoords;

    /**
     * Create a successful status object.
     * @param coords The coordinates reflecting the mutation
     */
    public MutationStatus(VBucketCoordinates coords) {
        ec = ErrorCode.SUCCESS;
        vbCoords = coords;
    }

    /**
     * Create a failed status object
     * @param code The failure status.
     */
    public MutationStatus(ErrorCode code) {
        ec = code;
        vbCoords = new BasicVBucketCoordinates(0, 0);
    }

    public ErrorCode getStatus() {
        return ec;
    }
    public VBucketCoordinates getCoords() {
        return vbCoords;
    }
}
