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

public class EndureRequest extends KeyAccessRequest {
    /**
     * EndureRequest is used to fake the persistence of a key-value pair
     * on a number of nodes.
     *
     * @param key The key to endure
     * @param value An optional value
     * @param onMaster Should it be persisted on the master?
     * @param numReplicas THe number of replicas to persist it
     */
    public EndureRequest(@NotNull String key, @NotNull String value, boolean onMaster, int numReplicas) {
        this(key, value, onMaster, numReplicas, "");
    }

    public EndureRequest(@NotNull String key, @NotNull String value, boolean onMaster, List<Integer> replicaIds) {
        this(key, value, onMaster, replicaIds, "");
    }

    public EndureRequest(@NotNull String key, @NotNull String value, boolean onMaster, int numReplicas, @NotNull String bucket) {
        super(key, value, onMaster, numReplicas, 0, bucket);
        command.put("command", "endure");
    }

    public EndureRequest(@NotNull String key, @NotNull String value, boolean onMaster, List<Integer> replicaIds, @NotNull String bucket) {
        super(key, value, onMaster, replicaIds, 0, bucket);
        command.put("command", "endure");
    }
}
