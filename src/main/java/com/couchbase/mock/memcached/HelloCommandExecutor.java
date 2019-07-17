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

package com.couchbase.mock.memcached;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryHelloCommand;
import com.couchbase.mock.memcached.protocol.BinaryHelloResponse;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Created by mnunberg on 2/4/15.
 */
public class HelloCommandExecutor implements CommandExecutor {
    @Override
    public BinaryResponse execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        BinaryHelloCommand hcmd = (BinaryHelloCommand) cmd;
        // Get the features
        client.setSupportedFeatures(hcmd.getFeatures());
        boolean[] featuresSparse = client.getSupportedFeatures();
        // Get length of required array
        int numFeatures = 0;
        for (boolean b : featuresSparse) {
            if (b) {
                numFeatures++;
            }
        }
        int outIndex = 0;
        int[] featuresArray = new int[numFeatures];
        for (int i = 0; i < featuresSparse.length; i++) {
            if (featuresSparse[i]) {
                featuresArray[outIndex++] = i;
            }
        }
        // HELLO response should not be ever traced
        client.sendResponse(new BinaryHelloResponse(hcmd, featuresArray));
        return null;
    }
}
