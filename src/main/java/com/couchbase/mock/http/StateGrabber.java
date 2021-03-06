/*
 * Copyright 2017 Couchbase, Inc.
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
package com.couchbase.mock.http;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.JsonUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to extract a consistent state for various configuration
 * parameters. All methods are static methods.
 *
 * @author M. Nunberg
 */
class StateGrabber {

    /*
     * Locking not needed here (yet)
     * Handler for /pools$
     */
    static String getAllPoolsJSON(CouchbaseMock mock) {
        Map<String,Object> retObj = new HashMap<String, Object>();
        List<Object> pools = new ArrayList<Object>();
        Map<String,Object> defaultPool = new HashMap<String, Object>();

        defaultPool.put("name", mock.getPoolName());
        defaultPool.put("uri", "/pools/" + mock.getPoolName());
        defaultPool.put("streamingUri", "/poolsStreaming/" + mock.getPoolName());

        pools.add(defaultPool);
        retObj.put("pools", pools);
        retObj.put("isAdminCreds", Boolean.TRUE);
        retObj.put("implementationVersion", "CouchbaseMock");
        return JsonUtils.encode(retObj);
    }

    static String getPoolInfoJSON(CouchbaseMock mock) {
        Map<String, Object> poolInfo = new HashMap<String, Object>();
        HashMap<String, Object> buckets = new HashMap<String, Object>();

        poolInfo.put("buckets", buckets);
        buckets.put("uri", "/pools/" + mock.getPoolName() + "/buckets");
        buckets.put("streamingUri", "/pools/" + mock.getPoolName() + "/bucketsStreaming/");
        buckets.put("terseBucketsBase", "/pools/" + mock.getPoolName() + "/b/");
        buckets.put("terseStreamingBucketsBase", "/pools/" + mock.getPoolName() + "/bs/");
        return JsonUtils.encode(poolInfo);
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

    static String getAllBucketsJSON(List<Bucket> allowedBuckets) {
        List<Map> bucketsJSON = new ArrayList<Map>();
        for (Bucket bucket : allowedBuckets) {
            bucketsJSON.add(bucket.getConfigMap());
        }
        return JsonUtils.encode(bucketsJSON);
    }
}