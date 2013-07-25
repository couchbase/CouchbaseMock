/*
 * Copyright 2012 Couchbase, Inc.
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
package org.couchbase.mock.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;

/**
 * Utility class to extract a consistent state for various configuration
 * parameters. All methods are static methods.
 *
 * @author M. Nunberg
 */
public class StateGrabber {

    /*
     * Locking not needed here (yet)
     */
    static String getAllPoolsJSON(CouchbaseMock mock) {
        Map<String, Object> pools = new HashMap<String, Object>();
        pools.put("name", mock.getPoolName());
        pools.put("uri", "/pools/" + mock.getPoolName());
        pools.put("streamingUri", "/poolsStreaming/" + mock.getPoolName());

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("pools", pools);
        map.put("isAdminCreds", Boolean.TRUE);
        return JSONObject.fromObject(map).toString();
    }

    static String getPoolJSON(CouchbaseMock mock, String poolName) {
        Map<String, Object> poolInfo = new HashMap<String, Object>();
        HashMap<String, Object> buckets = new HashMap<String, Object>();
        poolInfo.put("buckets", buckets);
        buckets.put("uri", "/pools/" + mock.getPoolName() + "/buckets");
        return JSONObject.fromObject(poolInfo).toString();
    }

    static String getBucketJSON(Bucket bucket) {
        String ret;
        bucket.configReadLock();
        try {
            ret = bucket.getJSON();
        } finally {
            bucket.configReadUnlock();
        }
        return ret;
    }

    static String getAllBucketsJSON(CouchbaseMock mock, String poolName, List<Bucket> allowedBuckets) {
        JSONArray bucketsJSON = new JSONArray();
        for (Bucket bucket : allowedBuckets) {
            bucketsJSON.add(getBucketJSON(bucket));
        }
        return bucketsJSON.toString();
    }

    static String getStreamDelimiter() {
        return "\n\n\n\n";
    }
}