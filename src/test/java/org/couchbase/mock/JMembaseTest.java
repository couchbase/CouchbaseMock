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
package org.couchbase.mock;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.TestCase;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.Map;


import org.couchbase.mock.client.*;
import org.couchbase.mock.util.Base64;
import org.couchbase.mock.harakiri.HarakiriMonitor;
import org.couchbase.mock.util.ReaderUtils;


/**
 * Basic testing of JMembase
 *
 * @author Trond Norbye
 */
public class JMembaseTest extends TestCase {

    public JMembaseTest(String testName) {
        super(testName);
    }

    private CouchbaseMock instance;
    static private final int port = 28091;
    static private final int numNodes;
    static private final int numVBuckets;

    static {
        final String platform = System.getProperty("os.name");
        if (platform.equals("Mac OS X") || platform.equals("Linux")) {
            numNodes = 4;
            numVBuckets = 16;
        } else {
            numNodes = 100;
            numVBuckets = 1024;
        }
    }

    private boolean serverNotReady(int port) {
        Socket socket = null;
        try {
            socket = new Socket("localhost", port);
            return false;
        } catch (UnknownHostException ex) {
        } catch (IOException ex) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        return true;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        instance = new CouchbaseMock(null, port, numNodes, numVBuckets, "default:,protected:secret");
        instance.start();
        do {
            Thread.sleep(100);
        } while (serverNotReady(port));
    }

    @Override
    protected void tearDown() throws Exception {
        instance.stop();
        super.tearDown();
    }

    public void testHandleHttpRequest() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode("protected:secret"));
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testHandleHttpRequestWithTrailingSlash() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected/");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode("protected:secret"));
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testAdministratorCouldAccessProtectedBuckets() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode("Administrator:password"));
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testDefaultBucketShouldBeAccessibleForEveryone() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testProtectedBucketsShouldBeFilteredOutFromList() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            StringBuilder sb = new StringBuilder();
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                sb.append(line);
            }
            List json = JsonUtils.decodeAsList(sb.toString());
            assertEquals(1, json.size());
            Map<String,Object> bucket = (Map<String,Object>)json.get(0);
            assertEquals("default", bucket.get("name"));
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testHandleHttpRequestNetwork() throws IOException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.print("GET /pools/default/buckets/default HTTP/1.1\r\n");
        pw.print("Authorization: Basic " + Base64.encode("Administrator:password") + "\r\n");
        pw.print("\r\n");
        pw.flush();

        Socket s = new Socket("localhost", port);
        OutputStream out = s.getOutputStream();
        out.write(sw.toString().getBytes());
        out.flush();

        BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
        int content_length = 0;
        String header;
        while ((header = in.readLine()).length() > 0) {
            String[] tokens = header.split(": ");
            if (tokens[0].equals("Content-Length")) {
                content_length = Integer.parseInt(tokens[1]);
            }
        }
        char[] body = new char[content_length];
        assertEquals(content_length, in.read(body));
        s.close();
    }

    public void testHandleHttpRequestMissingAuth() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    public void testHandleHttpRequestIncorrectCred() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode("Bubba:TheHut"));
            assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

    }

    @SuppressWarnings("SpellCheckingInspection")
    public void testHandleHttpRequestIllegalCred() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode(":TheHut"));
            assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

    }

    public void testHandleHttpRequestUnkownFile() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode("Administrator:password"));
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

//    @SuppressWarnings("UnusedAssignment")
//    public void testHarakiriMonitorInvalidHost() throws IOException {
//        try {
//            HarakiriMonitor m = new HarakiriMonitor("ItWouldSuckIfYouHadAHostNamedThis", 0, false, instance.getDispatcher());
//            fail("I was not expecting to be able to connect to: \"ItWouldSuckIfYouHadAHostNamedThis:0\"");
//        } catch (Throwable t) {
//        }
//    }

    @SuppressWarnings("UnusedAssignment")
    public void testHarakiriMonitorInvalidPort() throws IOException {
        try {
            HarakiriMonitor m = new HarakiriMonitor(instance.getDispatcher());
            m.connect(null, 0);
            fail("I was not expecting to be able to connect to port 0");
        } catch (Throwable t) {
        }
    }

    public void testHarakiriMonitor() throws IOException {
        ServerSocket server = new ServerSocket(0);
        HarakiriMonitor m;
        m = new HarakiriMonitor(instance.getDispatcher());
        m.connect(null, server.getLocalPort());

        Thread t = new Thread(m);
        t.start();
        server.close();

        while (t.isAlive()) {
            try {
                t.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(JMembaseTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private String readConfig(InputStream stream) throws IOException {
        int bb, lf = 0;
        StringBuilder cfg = new StringBuilder();

        do {
            bb = stream.read();
            if (bb == '\n') {
                lf++;
            } else {
                lf = 0;
                cfg.append(bb);
            }
        } while (lf < 4);
        return cfg.toString();
    }

    private static String readInput(InputStream cin) throws IOException {
        byte[] inputBuffer = new byte[4096];
        int nr;
        StringBuilder sb = new StringBuilder();

        while ((nr = cin.read(inputBuffer)) > 0) {
            String s = new String(inputBuffer, 0, nr);
            sb.append(s);
            if (nr < inputBuffer.length) {
                break;
            }
        }
        return sb.toString();
    }

    private boolean readResponse(InputStream in) throws IOException {
        String json = readInput(in);
        try {
            JsonObject response = (new Gson()).fromJson(json, JsonObject.class);
            return response.get("status").getAsString().toLowerCase().equals("ok");
        } catch (Throwable ex) {
            System.err.println("Invalid response received from the server: [" + json + "]");
            return false;
        }
    }

    @SuppressWarnings("SpellCheckingInspection")
    public void testConfigStreaming() throws IOException {
        MockClient mock = new MockClient(new InetSocketAddress("localhost", 0));
        instance.startHarakiriMonitor("localhost:" + mock.getPort(), false);
        mock.negotiate();

        Bucket bucket = instance.getBuckets().get("protected");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/bucketsStreaming/protected");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("Authorization", "Basic " + Base64.encode("protected:secret"));
        InputStream stream = conn.getInputStream();
        String currCfg, nextCfg;

        currCfg = readConfig(stream);
        assertEquals(numNodes, bucket.activeServers().size());

        assertTrue(mock.request(new FailoverRequest(1, "protected")).isOk());
        nextCfg = readConfig(stream);
        assertNotSame(currCfg, nextCfg);
        assertEquals(numNodes - 1, bucket.activeServers().size());
        currCfg = nextCfg;

        assertTrue(mock.request(new RespawnRequest(1, "protected")).isOk());
        nextCfg = readConfig(stream);
        assertNotSame(currCfg, nextCfg);
        assertEquals(numNodes, bucket.activeServers().size());

        assertTrue(mock.request(new HiccupRequest(10000, 20)).isOk());
        assertTrue(mock.request(new FailoverRequest(1)).isOk());
        assertTrue(mock.request(new RespawnRequest(1)).isOk());
        mock.shutdown();

        instance.getMonitor().stop();
    }

    public void testIllegalMockCommand() throws IOException {
        ServerSocket server = new ServerSocket(0);
        instance.startHarakiriMonitor("localhost:" + server.getLocalPort(), false);
        Socket client = server.accept();
        InputStream input = client.getInputStream();
        OutputStream output = client.getOutputStream();
        readInput(input);

        output.write("Yo, this should fail!\n".getBytes());
        assertFalse(readResponse(input));
    }

    public void testUnknownMockCommand() throws IOException {
        MockClient mock = new MockClient(new InetSocketAddress("localhost", 0));
        instance.startHarakiriMonitor("localhost:" + mock.getPort(), false);
        mock.negotiate();
        MockRequest request = MockRequest.build("foo");
        MockResponse resp = mock.request(request);
        assertFalse(resp.isOk());
        assertNotNull(resp.getErrorMessage());
    }

    public void testRestFlush() throws IOException {
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default/controller/doFlush");
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    final static String DDOC = "{"
            + "  \"_id\": \"_design/beer\","
            + "  \"language\": \"javascript\","
            + "  \"views\": {"
            + "    \"all\": {"
            + "      \"map\": \"function(doc){ emit(doc.id, null); }\""
            + "    }"
            + "  }"
            + "}";


    public void testDesignManagement() throws Exception {
        URL url = new URL("http://localhost:"+instance.getHttpPort()+"/default/_design/beer");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);

        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write(DDOC);
        osw.flush();
        osw.close();
        conn.getInputStream().close();

        // Get it back
        conn = (HttpURLConnection) url.openConnection();
        String s = ReaderUtils.fromStream(conn.getInputStream());
        assertEquals(s, DDOC);
    }


}
