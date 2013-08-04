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
package org.couchbase.mock.client;

import org.jetbrains.annotations.NotNull;

import java.util.List;

abstract class KeyAccessRequest extends MockRequest {
    KeyAccessRequest(@NotNull String key, @NotNull String value, boolean onMaster, int numReplicas, long cas, @NotNull String bucket) {
        super();
        payload.put("Key", key);
        if (!value.isEmpty()) {
            payload.put("Value", value);
        }
        if (!bucket.isEmpty()) {
            payload.put("Bucket", bucket);
        }
        if (cas != 0) {
            payload.put("CAS", cas);
        }
        payload.put("OnMaster", onMaster);
        payload.put("OnReplicas", numReplicas);
        command.put("payload", payload);
    }

    KeyAccessRequest(@NotNull String key, @NotNull String value, boolean onMaster, @NotNull List<Integer> replicaIds, long cas, @NotNull String bucket) {
        super();
        payload.put("Key", key);
        if (!value.isEmpty()) {
            payload.put("Value", value);
        }
        if (!bucket.isEmpty()) {
            payload.put("Bucket", bucket);
        }
        if (cas != 0) {
            payload.put("CAS", cas);
        }
        payload.put("OnMaster", onMaster);
        payload.put("OnReplicas", replicaIds);
        command.put("payload", payload);
    }

}
