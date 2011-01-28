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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @todo We should really refactor this code ;)
 * Its public so that I can use it from my tests...
 *
 * @author Trond Norbye
 */
public class HttpRequestImpl implements HttpRequest {

    private List<String> headers;
    private HttpReasonCode reasonCode;
    private boolean chunkedResponse;
    private ByteArrayOutputStream out;
    private List<ByteBuffer> output;
    private URI uri;

    public static HttpRequestImpl parse(ByteBuffer request) {
        // Search for a complete header...
        ByteArrayInputStream bais = new ByteArrayInputStream(request.array(), 0, request.position());
        BufferedReader r = new BufferedReader(new InputStreamReader(bais));

        HttpRequestImpl ret = null;
        try {
            ret = new HttpRequestImpl(new BufferedReader(r));
            request.rewind();
        } catch (Exception ex) {
            // do something smart ;)
        }

        return ret;
    }

    public HttpRequestImpl(BufferedReader input) throws Exception {
        reasonCode = HttpReasonCode.OK;
        headers = new ArrayList<String>();
        String s;
        while ((s = input.readLine()) != null) {
            if (s.length() == 0) {
                // header separator
                break;
            }
            if (Character.isSpaceChar(s.charAt(0))) {
                String o = headers.get(headers.size() - 1);
                headers.set(headers.size() - 1, o + s.trim());
            } else {
                headers.add(s);
            }
        }

        if (s == null) {
            throw new Exception("Need data");
        }

        String request = headers.get(0);
        String tokens[] = request.split(" ");
        if (tokens.length != 3 || !tokens[2].equals("HTTP/1.1")) {
            reasonCode = HttpReasonCode.HTTP_Version_not_supported;
            return;
        }

        if (!tokens[0].equalsIgnoreCase("GET")) {
            reasonCode = HttpReasonCode.Not_Implemented;
            return;
        }

        uri = new URI(tokens[1]);
    }

    public HttpReasonCode getReasonCode() {
        return reasonCode;
    }

    public URI getRequestedUri() {
        return uri;
    }

    public void setReasonCode(HttpReasonCode reasonCode) {
        this.reasonCode = reasonCode;
    }

    public void setChunkedResponse(boolean chunkedResponse) {
        this.chunkedResponse = chunkedResponse;
    }

    public OutputStream getOutputStream() {
        if (out == null) {
            out = new ByteArrayOutputStream();
        }
        return out;
    }

    public String getHeader(String string) {
        for (String s : headers) {
            if (s.startsWith(string)) {
                return s;
            }
        }
        return null;
    }

    public void resetResponse() {
        reasonCode = HttpReasonCode.OK;
        chunkedResponse = false;
        out = null;
    }

    public void encodeResponse() {
        if (output == null) {
            output = new ArrayList<ByteBuffer>();

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.print("HTTP/1.1 ");
            pw.print(reasonCode.value());
            pw.print(' ');
            pw.print(reasonCode.toString() + "\r\n");

            if (chunkedResponse) {
                pw.print("Transfer-Encoding: chunked\r\n");
            }

            // End header section
            pw.print("\r\n");
            pw.flush();
            ByteBuffer buffer = ByteBuffer.wrap(sw.getBuffer().toString().getBytes());
            output.add(buffer);
        }

        if (out != null) {
            if (chunkedResponse) {
                byte array[] = out.toByteArray();
                output.add(ByteBuffer.wrap(Integer.toHexString(array.length).getBytes()));
                output.add(ByteBuffer.wrap("\r\n".getBytes()));
                output.add(ByteBuffer.wrap(array));
                output.add(ByteBuffer.wrap("\r\n".getBytes()));
            } else {
                output.add(ByteBuffer.wrap(out.toByteArray()));
            }
            out = null;
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

    public List<ByteBuffer> getResponseBuffers() {
        purgeSendBuffers();
        return output;
    }

    public boolean shouldClose() {
        purgeSendBuffers();
        return (output.isEmpty() && (reasonCode != HttpReasonCode.OK || !chunkedResponse));
    }

    public void startChunk() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void endChunk() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
