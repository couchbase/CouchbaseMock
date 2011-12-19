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


/**
 * Basic testing of JMembase
 *
 * @author Trond Norbye
 */
public class JMembaseTest extends TestCase {

    public JMembaseTest(String testName) {
        super(testName);
    }
    CouchbaseMock instance;
    int port = 18091;
    Thread thread;

    private boolean serverIsReady(String host, int port) {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            return true;
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
        return false;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        instance = new CouchbaseMock(null, port, 100, 4096, "default:,protected:secret");
        instance.start();
        do {
            Thread.sleep(100);
        } while (!serverIsReady("localhost", port));
    }

    @Override
    protected void tearDown() throws Exception {
        instance.stop();
        super.tearDown();
    }

    public void testHandleHttpRequest() throws IOException {
        System.out.println("testHandleHttpRequest");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn = null;

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
        System.out.println("testAdministratorCouldAccessProtectedBuckets");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn = null;

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
        System.out.println("testDefaultBucketShouldBeAccessibleForEveryone");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default");
        HttpURLConnection conn = null;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testProtectedBucketsShouldBeFilteredOutFromList() throws IOException {
        System.out.println("testProtectedBucketsShouldBeFilteredOutFromList");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets");
        HttpURLConnection conn = null;

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
        System.out.println("testHandleHttpRequestNetwork");
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
        System.out.println("testHandleHttpRequestMissingAuth");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn = null;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testHandleHttpRequestIncorrectCred() throws IOException {
        System.out.println("testHandleHttpRequestIncorrectCred");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/protected");
        HttpURLConnection conn = null;

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
        System.out.println("testHandleHttpRequestUnkownFile");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/");
        HttpURLConnection conn = null;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode("Administrator:password"));
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public void testHarakiriMonitorInvalidHost() throws IOException {
        System.out.println("testHarakiriMonitorInvalidHost");
        try {
            CouchbaseMock.HarakiriMonitor m = new CouchbaseMock.HarakiriMonitor("ItWouldSuckIfYouHadAHostNamedThis", 0, port, false, null);
            fail("I was not expecting to be able to connect to: \"ItWouldSuckIfYouHadAHostNamedThis:0\"");
        } catch (Throwable t) {
        }
    }

    public void testHarakiriMonitorInvalidPort() throws IOException {
        System.out.println("testHarakiriMonitorInvalidPort");
        try {
            CouchbaseMock.HarakiriMonitor m = new CouchbaseMock.HarakiriMonitor(null, 0, port, false, null);
            fail("I was not expecting to be able to connect to port 0");
        } catch (Throwable t) {
        }
    }

    public void testHarakiriMonitor() throws IOException {
        System.out.println("testHarakiriMonitor");
        ServerSocket server = new ServerSocket(0);
        CouchbaseMock.HarakiriMonitor m;
        m = new CouchbaseMock.HarakiriMonitor(null, server.getLocalPort(), port, false, null);

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

    private String readConfig(InputStream stream) {
        int bb, lf = 0;
        StringBuilder cfg = new StringBuilder();

        do {
            try {
                bb = stream.read();
                if (bb == '\n') {
                    lf++;
                } else {
                    lf = 0;
                    cfg.append(bb);
                }
            } catch (IOException ex) {
                Logger.getLogger(JMembaseTest.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        } while (lf < 4);
        return cfg.toString();
    }

    public void testConfigStreaming() throws IOException {
        System.out.println("testConfigStreaming");
        ServerSocket server = new ServerSocket(0);
        CouchbaseMock.HarakiriMonitor m = new CouchbaseMock.HarakiriMonitor(null, server.getLocalPort(), port, true, instance);
        Thread t = new Thread(m, "HarakiriMonitor");
        t.start();
        Socket client = server.accept();
        InputStream cin = client.getInputStream();
        OutputStream cout = client.getOutputStream();
        StringBuilder rport = new StringBuilder();
        char cc;
        while ((cc = (char) cin.read()) > 0) {
            rport.append(cc);
        }
        assertEquals(rport.toString(), Integer.toString(port));

        Bucket defaultBucket = instance.getBuckets().get("default");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/bucketsStreaming/default");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("Authorization", "Basic " + Base64.encode("Administrator:password"));
        InputStream stream = conn.getInputStream();
        String currCfg, nextCfg;

        currCfg = readConfig(stream);
        assertEquals(100, defaultBucket.activeServers().size());

        cout.write("failover,1,default\n".getBytes());
        nextCfg = readConfig(stream);
        assertNotSame(currCfg, nextCfg);
        assertEquals(99, defaultBucket.activeServers().size());
        currCfg = nextCfg;

        cout.write("respawn,1,default\n".getBytes());
        nextCfg = readConfig(stream);
        assertNotSame(currCfg, nextCfg);
        assertEquals(100, defaultBucket.activeServers().size());

        server.close();
        t.interrupt();
    }
}
