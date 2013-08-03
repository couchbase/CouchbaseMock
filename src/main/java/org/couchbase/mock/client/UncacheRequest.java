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

public class UncacheRequest extends KeyAccessRequest {
    public UncacheRequest(@NotNull String key, boolean onMaster, int numReplicas) {
        this(key, onMaster, numReplicas, "");
    }

    public UncacheRequest(@NotNull String key, boolean onMaster, @NotNull List<Integer> replicaIds) {
        this(key, onMaster, replicaIds, "");
    }

    public UncacheRequest(@NotNull String key, boolean onMaster, int numReplicas, @NotNull String bucket) {
        super(key, "", onMaster, numReplicas, 0, bucket);
        command.put("command", "uncache");
    }

    public UncacheRequest(@NotNull String key, boolean onMaster, @NotNull List<Integer> replicaIds, @NotNull String bucket) {
        super(key, "", onMaster, replicaIds, 0, bucket);
        command.put("command", "uncache");
    }
}
