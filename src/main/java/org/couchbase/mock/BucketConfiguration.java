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

public class BucketConfiguration {
    public int numVBuckets = 1024;
    public int numNodes = 10;
    public int numReplicas = 2;
    public Bucket.BucketType type = Bucket.BucketType.COUCHBASE;
    public String poolName = "default";
    public String name;
    public String password = "";
    public String hostname = "localhost";
    public int port = 0;
    public int bucketStartPort = 0;

    @SuppressWarnings("SimplifiableIfStatement")
    public boolean validate() {
        if (name ==  null) {
            return false;
        }
        return !(port < 0 || bucketStartPort < 0);
    }

    public BucketConfiguration() {

    }

    // Creates a new BucketConfiguration setting the defaults from another
    // object.
    // Note that this does not set the name or password of the bucket, which
    // must still be set explicitly
    public BucketConfiguration(BucketConfiguration other) {
        numVBuckets = other.numVBuckets;
        numNodes = other.numNodes;
        numReplicas = other.numReplicas;
        type = other.type;
        poolName = other.poolName;
        hostname = other.hostname;
        port = other.port;
        bucketStartPort = other.bucketStartPort;
    }
}
