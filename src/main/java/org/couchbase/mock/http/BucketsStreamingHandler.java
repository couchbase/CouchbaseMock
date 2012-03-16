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

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock.HarakiriMonitor;

/**
 *
 * @author M. Nunberg
 */
public class BucketsStreamingHandler implements Observer {

    private final OutputStream output;
    private final Bucket bucket;
    private final HarakiriMonitor monitor;
    private final CountDownLatch completed;
    private final Lock updateHandlerLock;

    public BucketsStreamingHandler(HarakiriMonitor monitor, Bucket bucket, OutputStream output) {
        this.output = output;
        this.bucket = bucket;
        this.monitor = monitor;
        this.completed = new CountDownLatch(1);
        updateHandlerLock = new ReentrantLock();
    }

    private byte[] getConfigBytes() {
        StringWriter sw = new StringWriter();
        sw.append(StateGrabber.getBucketJSON(bucket));
        sw.append(StateGrabber.getStreamDelimiter());
        return sw.toString().getBytes();
    }

    @Override
    public void update(Observable o, Object arg) {
        updateHandlerLock.lock();
        try {
            /*
             * getConfigBytes acquires a lock while retrieving the info
             */
            output.write(getConfigBytes());
            output.flush();
        }
        catch (IOException ex) {
            completed.countDown();
        } finally {
            updateHandlerLock.unlock();
        }
    }

    public void startStreaming() throws IOException, InterruptedException {
        byte[] configBytes;
        bucket.configReadLock();
        configBytes = getConfigBytes();


        // Lock the updater lock, make sure updates are not received
        // before we send our initial (older) data.
        updateHandlerLock.lock();

        if (monitor != null) {
            monitor.addObserver(this);
        }

        // This can be unlocked, because we have wired the update() method.
        // Therefore, any changes will be placed AFTER our 'initial' frozen
        // snapshot.
        bucket.configReadUnlock();

        try {
            output.write(configBytes);
            output.flush();
        } finally {
            // Allow any subsequent updates to be sent
            updateHandlerLock.unlock();
        }

        completed.await();
        /*
         * Error or somesuch
         */
        output.close();
    }
}