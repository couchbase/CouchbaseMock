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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.couchbase.mock.util.Base64;
import org.couchbase.mock.harakiri.HarakiriMonitor;


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
    private final int port = 18091;
    private int numNodes = 100;
    private int numVBuckets = 4096;

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
        if (System.getProperty("os.name").equals("Mac OS X")) {
            numNodes = 4;
            numVBuckets = 10;
        }

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
            JSONArray json = (JSONArray)JSONSerializer.toJSON(sb.toString());
            assertEquals(1, json.size());
            JSONObject bucket = (JSONObject) json.get(0);
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

    @SuppressWarnings("UnusedAssignment")
    public void testHarakiriMonitorInvalidHost() throws IOException {
        try {
            HarakiriMonitor m = new HarakiriMonitor("ItWouldSuckIfYouHadAHostNamedThis", 0, false, instance);
            fail("I was not expecting to be able to connect to: \"ItWouldSuckIfYouHadAHostNamedThis:0\"");
        } catch (Throwable t) {
        }
    }

    @SuppressWarnings("UnusedAssignment")
    public void testHarakiriMonitorInvalidPort() throws IOException {
        try {
            HarakiriMonitor m = new HarakiriMonitor(null, 0, false, instance);
            fail("I was not expecting to be able to connect to port 0");
        } catch (Throwable t) {
        }
    }

    public void testHarakiriMonitor() throws IOException {
        ServerSocket server = new ServerSocket(0);
        HarakiriMonitor m;
        m = new HarakiriMonitor(null, server.getLocalPort(), false, instance);

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

    @SuppressWarnings("SpellCheckingInspection")
    public void testConfigStreaming() throws IOException {
        ServerSocket server = new ServerSocket(0);
        instance.setupHarakiriMonitor("localhost:" + server.getLocalPort(), false);
        Socket client = server.accept();
        InputStream cin = client.getInputStream();
        OutputStream cout = client.getOutputStream();
        StringBuilder rport = new StringBuilder();
        char cc;
        while ((cc = (char) cin.read()) > 0) {
            rport.append(cc);
        }
        assertEquals(rport.toString(), Integer.toString(port));

        Bucket bucket = instance.getBuckets().get("protected");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/bucketsStreaming/protected");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("Authorization", "Basic " + Base64.encode("protected:secret"));
        InputStream stream = conn.getInputStream();
        String currCfg, nextCfg;

        currCfg = readConfig(stream);
        assertEquals(numNodes, bucket.activeServers().size());

        cout.write("failover,1,protected\n".getBytes());
        nextCfg = readConfig(stream);
        assertNotSame(currCfg, nextCfg);
        assertEquals(numNodes - 1, bucket.activeServers().size());
        currCfg = nextCfg;

        cout.write("respawn,1,protected\n".getBytes());
        nextCfg = readConfig(stream);
        assertNotSame(currCfg, nextCfg);
        assertEquals(numNodes, bucket.activeServers().size());

        cout.write("hiccup,10000,20\n".getBytes());
        server.close();
        instance.getMonitor().stop();
    }
}
