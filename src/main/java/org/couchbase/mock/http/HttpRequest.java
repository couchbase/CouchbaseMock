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
package org.couchbase.mock.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * The interface representing a HTTP request/response.
 *
 * @author Trond Norbye
 */
public interface HttpRequest {
    /**
     * Get a specific header from the HTTP request
     *
     * @param string The name of the header field
     * @return The complete header line
     */
    public String getHeader(String string);

    /**
     * Get an output stream to add data to the content of the response
     * @return Output stream to write to the body
     */
    public OutputStream getOutputStream();

    /**
     * Get the URI to serve
     * @return
     */
    public URI getRequestedUri();

    /**
     * Reset the response (throw away all modifications to the response and
     * start on scratch)
     */
    public void resetResponse();

    /**
     * Enable / disable Chunked response..
     * @param chunkedResponse
     */
    public void setChunkedResponse(boolean chunkedResponse);

    /**
     * Set the reason code for the reply. The default value for the reason
     * code in the response is OK (200)
     *
     * @param reasonCode
     */
    public void setReasonCode(HttpReasonCode reasonCode);

    /**
     * Start a new chunk
     * @throws IOException if the client is no longer connected
     */
    public void startChunk() throws IOException;

    /**
     * End this chunk. The chunk of data will be transferred as soon as
     * possible...
     * @throws IOException if the client is no longer connected
     */
    public void endChunk() throws IOException;
}
