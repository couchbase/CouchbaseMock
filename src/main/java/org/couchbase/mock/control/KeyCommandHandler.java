/*
 * Copyright 2013 Couchbase, Inc.
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
package org.couchbase.mock.control;

import org.couchbase.mock.harakiri.HarakiriCommand;
import com.google.gson.JsonObject;
import java.security.AccessControlException;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.harakiri.MissingRequiredFieldException;
import org.couchbase.mock.memcached.KeySpec;
import org.couchbase.mock.memcached.VBucketInfo;

/**
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public abstract class KeyCommandHandler extends HarakiriCommand {
    protected KeySpec keySpec;
    protected Bucket bucket;
    protected VBucketInfo vbi;

    public KeyCommandHandler(CouchbaseMock m) {
        super(m);
    }

    @Override
    protected void handleJson(JsonObject json) {
        super.handleJson(json);
        short vbIndex = -1;
        String key;

        String bucketString = "default";
        if (payload.has("Bucket")) {
            bucketString = payload.get("Bucket").getAsString();
        }

        bucket = mock.getBuckets().get(bucketString);
        if (bucket == null) {
            throw new AccessControlException("No such bucket: " + bucketString);
        }

        if (!payload.has("Key")) {
            throw new MissingRequiredFieldException("Key");
        }

        key = payload.get("Key").getAsString();
        if (!payload.has("vBucket")) {
            vbIndex = bucket.getVbIndexForKey(key);
        }

        if (vbIndex < 0 || vbIndex >= bucket.getVBucketInfo().length) {
            throw new AccessControlException("Invalid vBucket " + vbIndex);
        }

        keySpec = new KeySpec(key, vbIndex);
        vbi = bucket.getVBucketInfo()[keySpec.vbId];

    }
}
