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

package com.couchbase.mock.memcached.client;

import com.couchbase.mock.memcached.errormap.RetrySpec;

import java.io.IOException;

/**
 * @author Mark Nunberg
 */
public class Retryer {
    final MemcachedClient client;
    final RetrySpec spec;
    final byte[] cmd;

    public Retryer(MemcachedClient client, RetrySpec spec, byte[] cmd) {
        this.client = client;
        this.spec = spec;
        this.cmd = cmd;
    }

    private static void accuSleep(long sleepTime) throws InterruptedException {
        long timeAfterSleep = System.currentTimeMillis() + sleepTime;
        while (System.currentTimeMillis() < timeAfterSleep) {
            Thread.sleep(0, 500000);
        }
    }

    /**
     * Runs until the retry duration is reached
     *
     * @throws Exception if an error occurs
     */
    public void run() throws Exception {
        // Send the initial command:
        client.sendRequest(cmd);

        long endTime = System.currentTimeMillis() + spec.getMaxDuration();

        // Wait until the 'after' time
        Thread.sleep(spec.getAfter());

        int numAttempts = 0;
        long now = System.currentTimeMillis();

        while (now < endTime) {
            client.sendRequest(cmd);
            now = System.currentTimeMillis();

            numAttempts ++;

            // See how to retry:
            long sleepTime = 0;
            if (spec.isConstant()) {
                sleepTime = spec.getInterval();
            } else if (spec.isLinear()) {
                sleepTime = spec.getInterval() * numAttempts;
            } else if (spec.isExponential()) {
                sleepTime = (long) Math.pow(spec.getInterval(), numAttempts);
            }

            if (spec.getCeil() > 0) {
                sleepTime = Math.min(spec.getCeil(), sleepTime);
            }

            if (now + sleepTime > endTime) {
                break;
            } else {
                accuSleep(sleepTime);
                now = System.currentTimeMillis();
            }
        }
    }

    public void runError() throws Exception {
        for (int i = 0; i < 10; i++) {
            client.sendRequest(cmd);
        }
    }
}
