/*
 * Copyright 2013 mnunberg.
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
package org.couchbase.mock.memcached;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author mnunberg
 */
public class OutputContext {
    private List<ByteBuffer> buffers;
    private final ByteBuffer[] singleArray = new ByteBuffer[1];


    public ByteBuffer[] getIov() {
        if (buffers.size() == 1) {
            singleArray[0] = buffers.get(0);
            return singleArray;
        }
        return buffers.toArray(new ByteBuffer[buffers.size()]);
    }

    public boolean hasRemaining() {
        return !buffers.isEmpty();
    }
    /**
     * Get an OutputBuffer containing a subset of the current one
     * @param limit How many bytes should be available
     * @return a new OutputContext
     */
    public OutputContext getSlice(int limit) {
        List<ByteBuffer> newBufs = new LinkedList<ByteBuffer>();
        ByteBuffer buf = ByteBuffer.allocate(limit);
        Iterator<ByteBuffer> iter = buffers.iterator();

        while (iter.hasNext() && buf.position() < buf.limit()) {
            ByteBuffer cur = iter.next();
            int diff = buf.limit() - buf.position();
            if (diff > cur.limit()) {
                buf.put(cur);
                iter.remove();
            } else {
                ByteBuffer slice = cur.duplicate();
                slice.limit(diff);
                buf.put(slice);
            }
        }
        return new OutputContext(newBufs);
    }

    public void updateBytesSent(long num) {
        Iterator<ByteBuffer> iter = buffers.iterator();

        while (iter.hasNext()) {
            ByteBuffer cur = iter.next();
            if (cur.hasRemaining()) {
                break;
            }
            iter.remove();
        }
    }

    public OutputContext(List<ByteBuffer> origBufs) {
        buffers = origBufs;
    }

    public List<ByteBuffer> releaseRemaining() {
        List<ByteBuffer> ret = buffers;
        buffers = null;
        return ret;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("IOV: ")
                .append(buffers.size())
                .toString();
    }
}
