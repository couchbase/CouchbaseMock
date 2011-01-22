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
package org.membase.jmembase.http;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

/**
 * The HttpClient class is just a helper class to keep track of the
 * http session..
 *
 * @author Trond Norbye
 */
class HttpClient {

    private final ByteBuffer input;
    private final List<ByteBuffer> output;
    private final HttpRequestHandler requestHandler;
    private HttpRequestImpl request;

    public HttpClient(HttpRequestHandler requestHandler) {
        input = ByteBuffer.allocate(1024);
        output = new ArrayList<ByteBuffer>();
        this.requestHandler = requestHandler;
    }

    public ByteBuffer getInputBuffer() {
        return input;
    }

    public ByteBuffer[] getOutputBuffer() {
        if (output.isEmpty()) {
            return null;
        } else {
            ByteBuffer ret[] = new ByteBuffer[output.size()];
            output.toArray(ret);
            return ret;
        }
    }

    public int getSelectionKey() {
        if (output.isEmpty()) {
            return SelectionKey.OP_READ;
        } else {
            return SelectionKey.OP_READ | SelectionKey.OP_WRITE;
        }
    }

    private void purgeSendBuffers() {
        while (!output.isEmpty()) {
            ByteBuffer b = output.get(0);
            if (!b.hasRemaining()) {
                output.remove(0);
            } else {
                return;
            }
        }
    }

    boolean driveMachine() {
        // Remove all of the byte buffers we successfully sent
        purgeSendBuffers();

        if (input.position() > 0 && request == null) {
            // I've received some data..
            // @todo make this better.. for now just fake that everything is
            // OK!
            request = HttpRequestImpl.parse(input);
            if (request != null) {
                if (request.getReasonCode() == HttpReasonCode.OK) {
                    requestHandler.handleHttpRequest(request);
                }
                request.encodeResponse();
                output.addAll(request.getResponseBuffers());
            }
        } else if (request != null) {
            if (request.shouldClose()) {
                return false;
            }
            request.encodeResponse();
            output.addAll(request.getResponseBuffers());
        }

        return true;
    }
}
