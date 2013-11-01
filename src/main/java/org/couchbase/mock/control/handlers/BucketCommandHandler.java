/**
 *     Copyright 2012 Couchbase, Inc.
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
package org.couchbase.mock.control.handlers;

import com.google.gson.JsonObject;

import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.control.MockCommand;
import org.jetbrains.annotations.NotNull;

/**
 * This is an abstract class which operates on a specific
 * server on a specific bucket.
 *
 * @author M. Nunberg
 */
abstract public class BucketCommandHandler extends MockCommand {
    Bucket bucket;
    int idx;

    public @NotNull CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        idx = payload.get("idx").getAsInt();
        String bucketStr = "default";
        if (payload.has("bucket")) {
            bucketStr = payload.get("bucket").getAsString();
        }
        bucket = mock.getBuckets().get(bucketStr);
        // The return from this method is not returned back to the client
        return new CommandStatus().fail();
    }
}
