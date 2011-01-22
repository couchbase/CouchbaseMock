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
package org.membase.jmembase.memcached;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Trond Norbye
 */
public class MemcachedConnection {

    private BinaryProtocolHandler protocolHandler;
    private final byte header[];
    private BinaryCommand command;
    private final ByteBuffer input;
    private final List<ByteBuffer> output;

    public MemcachedConnection(MemcachedServer server) throws IOException {
        header = new byte[24];
        input = ByteBuffer.wrap(header);
        protocolHandler = server.getProtocolHandler();
        output = new ArrayList<ByteBuffer>();
    }

    public void step() throws IOException {
        if (!output.isEmpty()) {
            ByteBuffer b = output.get(0);
            if (!b.hasRemaining()) {
                output.remove(0);
            }
        }
        if (input.position() == header.length) {
            if (command == null) {
                command = new BinaryCommand(input);
            }

            if (command.complete()) {
                protocolHandler.execute(command, this);
                command = null;
                input.rewind();
            }
        }
    }

    public void sendResponse(BinaryResponse response) throws IOException {
        output.add(response.getBuffer());
    }

    boolean hasOutput() {
        return !output.isEmpty();
    }

    public ByteBuffer getInputBuffer() {
        if (command == null) {
            return input;
        } else {
            return command.getInputBuffer();
        }

    }

    public ByteBuffer getOutputBuffer() {
        if (output.isEmpty()) {
            return null;
        } else {
            return output.get(0);
        }
    }
}
