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
package org.couchbase.mock;

import org.jetbrains.annotations.NotNull;

public class BucketConfiguration {
    /** The number of VBuckets the bucket should contain */
    public int numVBuckets = 1024;

    /** The number of cluster nodes the bucket should have */
    public int numNodes = 10;

    /** The number of replicas for the bucket */
    public int numReplicas = 2;

    /** The type of the bucket (Couchbase or Memcached) */
    public Bucket.BucketType type = Bucket.BucketType.COUCHBASE;

    /** The name of the bucket. This field must be set when adding a new bucket */
    public String name;

    /** The password for the bucket. If no password is required, set this to the empty string (NOT null) */
    @NotNull
    public String password = "";

    /** The hostname the nodes should be bound to */
    public String hostname = "localhost";

    /** The port number the nodes should begin at. For example, if set to 1100 and {@link #numNodes} is set to
     * 5, then the nodes will listen on ports 1100 through 1104 */
    public int bucketStartPort = 0;

    public boolean validate() {
        return name != null && bucketStartPort >= 0;
    }

    /**
     * Creates an empty configuration with the default options
     */
    public BucketConfiguration() {

    }

    /**
     * Copies settings from the configuration {@code other} to a new object.
     * Note that bucket-specific settings, such as {@link #name}, {@link #password} and similar
     * are <b>not</b> copied.
     * @param other The configuration to copy
     */
    public BucketConfiguration(BucketConfiguration other) {
        numVBuckets = other.numVBuckets;
        numNodes = other.numNodes;
        numReplicas = other.numReplicas;
        type = other.type;
        hostname = other.hostname;
    }

    /** Gets the {@link #name } */
    public String getName() {
        return name;
    }

    @NotNull
    public String getPassword() {
        return password;
    }

    public Bucket.BucketType getType() {
        return type;
    }
}
