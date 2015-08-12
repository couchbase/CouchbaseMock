/*
 * Copyright 2015 Couchbase, Inc.
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

package org.couchbase.mock.memcached;

import org.couchbase.mock.Bucket;
import org.couchbase.mock.memcached.protocol.*;

/**
 * Created by mnunberg on 2/4/15.
 */
public class HelloCommandExecutor implements CommandExecutor {
    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        if (server.getBucket().getType() != Bucket.BucketType.COUCHBASE) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.NOT_SUPPORTED));
        }

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
        client.sendResponse(new BinaryHelloResponse(hcmd, featuresArray));
    }
}
