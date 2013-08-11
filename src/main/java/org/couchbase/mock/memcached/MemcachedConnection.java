/**
 *     Copyright 2011 Membase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.BinaryCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.LinkedList;

import org.couchbase.mock.memcached.protocol.CommandFactory;

/**
 * @author Trond Norbye
 */
class MemcachedConnection {

    private final BinaryProtocolHandler protocolHandler;
    private final byte header[];
    private BinaryCommand command;
    private final ByteBuffer input;
    private List<ByteBuffer> pending = new LinkedList<ByteBuffer>();
    private boolean authenticated;
    private boolean closed;

    public MemcachedConnection(MemcachedServer server) {
        closed = false;
        authenticated = server.getBucket().getPassword().length() <= 0;
        header = new byte[24];
        input = ByteBuffer.wrap(header);
        protocolHandler = server.getProtocolHandler();
    }

    public void step() throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        if (input.position() == header.length) {
            if (command == null) {
                command = CommandFactory.create(input);
            }

            if (command.complete()) {
                command.process();
                protocolHandler.execute(command, this);
                command = null;
                input.rewind();
            }
        }
    }

    public synchronized void sendResponse(BinaryResponse response) {
        if (pending == null) {
            pending = new LinkedList<ByteBuffer>();
        }
        pending.add(response.getBuffer());
    }

    boolean hasOutput() {
        if (pending == null) {
            return false;
        }

        if (pending.isEmpty()) {
            return false;
        }

        if (pending.get(0).hasRemaining() == false) {
            return false;
        }
        return true;
    }

    public ByteBuffer getInputBuffer() {
        if (command == null) {
            return input;
        } else {
            return command.getInputBuffer();
        }
    }

    public OutputContext borrowOutputContext() {
        if (!hasOutput()) {
            return null;
        }

        OutputContext ctx = new OutputContext(pending);
        pending = null;
        return ctx;
    }

    public void returnOutputContext(OutputContext ctx) {
        List<ByteBuffer> remaining = ctx.releaseRemaining();
        if (pending == null) {
            pending = remaining;
        } else {
            List<ByteBuffer> tmp = pending;
            pending = remaining;
            pending.addAll(tmp);
        }
    }


    void shutdown() {
        closed = true;
    }

    void setAuthenticated() {
        authenticated = true;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }
}
