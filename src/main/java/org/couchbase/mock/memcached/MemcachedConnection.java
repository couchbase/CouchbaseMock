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

import org.couchbase.mock.memcached.protocol.BinaryHelloCommand;
import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.BinaryCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

import org.couchbase.mock.memcached.protocol.CommandFactory;

/**
 * Class representing a single <i>client</i> connection to the server
 */
public class MemcachedConnection {

    private final BinaryProtocolHandler protocolHandler;
    private final byte header[];
    private BinaryCommand command;
    private final ByteBuffer input;
    private List<ByteBuffer> pending = new LinkedList<ByteBuffer>();
    private boolean authenticated;
    private boolean closed;
    private final MutationInfoWriter miw = new MutationInfoWriter();
    private boolean[] supportedFeatures = new boolean[BinaryHelloCommand.Feature.MAX.getValue()];

    public MemcachedConnection(MemcachedServer server) {
        closed = false;
        authenticated = server.getBucket().getPassword().length() <= 0;
        header = new byte[24];
        input = ByteBuffer.wrap(header);
        protocolHandler = server.getProtocolHandler();
    }

    /**
     * Attempt to process a single command from the input buffer. Note this does
     * not actually read from the socket.
     *
     * @throws IOException if the client has been closed
     */
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

    /**
     * Places the response into the current connection's output buffer.
     * Note that the actual I/O is not performed in this method
     * @param response the response to enqueue
     */
    public synchronized void sendResponse(BinaryResponse response) {
        if (pending == null) {
            pending = new LinkedList<ByteBuffer>();
        }
        pending.add(response.getBuffer());
    }

    /**
     * Determines whether this connection has pending responses to be sent
     * @return true  there are pending responses
     */
    boolean hasOutput() {
        if (pending == null) {
            return false;
        }

        if (pending.isEmpty()) {
            return false;
        }

        if (!pending.get(0).hasRemaining()) {
            return false;
        }
        return true;
    }

    /**
     * Gets the raw input buffer. This may be used to add additional request data
     * @return The input buffer
     */
    public ByteBuffer getInputBuffer() {
        if (command == null) {
            return input;
        } else {
            return command.getInputBuffer();
        }
    }

    /**
     * Temporarily borrow the head chunk of the output buffers. This may be used
     * to efficiently send responses or perform socket/buffer manipulation.
     *
     * When done with the context, ensure to call {@link #returnOutputContext(OutputContext)}
     * @return The output context
     */
    public OutputContext borrowOutputContext() {
        if (!hasOutput()) {
            return null;
        }

        OutputContext ctx = new OutputContext(pending);
        pending = null;
        return ctx;
    }

    /**
     * Re-transfer ownership of a given output buffer to the connection
     * @param ctx An OutputContext previously returned by {@link #borrowOutputContext()}
     */
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

    /**
     * Mark this connection has being closed. This will disallow further processing of commands
     */
    void shutdown() {
        closed = true;
    }

    /**
     * Mark this connection as having been successfully authenticated
     */
    void setAuthenticated() {
        authenticated = true;
    }

    /**
     * Check if this connection is authenticated
     * @return true if the client has already authenticated
     */
    public boolean isAuthenticated() {
        return authenticated;
    }

    public MutationInfoWriter getMutinfoWriter() {
        return miw;
    }

    public boolean[] getSupportedFeatures() {
        return Arrays.copyOf(supportedFeatures, supportedFeatures.length);
    }

    /**
     * Sets the supported features from a HELLO command.
     *
     * Note that the actual enabled features will be the ones supported by the mock
     * and also supported by the client. Currently the only supported feature is
     * MUTATION_SEQNO.
     *
     * @param input The features requested by the client.
     */
    void setSupportedFeatures(boolean[] input) {
        if (input.length != supportedFeatures.length) {
            throw new IllegalArgumentException("Bad features length!");
        }
        System.arraycopy(input, 0, supportedFeatures, 0, input.length);
        if (supportedFeatures[BinaryHelloCommand.Feature.MUTATION_SEQNO.getValue()]) {
            miw.setEnabled(true);
        } else {
            miw.setEnabled(false);
        }
        // Scan through all other features and disable them unless they are supported
        for (int i = 0; i < supportedFeatures.length; i++) {
            if (i == BinaryHelloCommand.Feature.MUTATION_SEQNO.getValue()) {
                // nothing;
            } else {
                supportedFeatures[i] = false;
            }
        }
    }
}
