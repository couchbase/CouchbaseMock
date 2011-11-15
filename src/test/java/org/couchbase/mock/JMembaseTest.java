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
        instance = new CouchbaseMock(null, port, 100, 4096);
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
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default");
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

    public void brokenTestHandleHttpRequestMissingAuth() throws IOException {
        System.out.println("testHandleHttpRequestMissingAuth");
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default");
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
        URL url = new URL("http://localhost:" + instance.getHttpPort() + "/pools/default/buckets/default");
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

    public void testHarakirMonitorInvalidHost() throws IOException {
        System.out.println("testHarakirMonitorInvalidHost");
        try {
            CouchbaseMock.HarakiriMonitor m = new CouchbaseMock.HarakiriMonitor("ItWouldSuckIfYouHadAHostNamedThis", 0, port, false);
            fail("I was not expecting to be able to connect to: \"ItWouldSuckIfYouHadAHostNamedThis:0\"");
        } catch (Throwable t) {
        }
    }

    public void testHarakirMonitorInvalidPort() throws IOException {
        System.out.println("testHarakirMonitorInvalidPort");
        try {
            CouchbaseMock.HarakiriMonitor m = new CouchbaseMock.HarakiriMonitor(null, 0, port, false);
            fail("I was not expecting to be able to connect to port 0");
        } catch (Throwable t) {
        }
    }

    public void testHarakirMonitor() throws IOException {
        System.out.println("testHarakirMonitor");
        ServerSocket server = new ServerSocket(0);
        CouchbaseMock.HarakiriMonitor m;
        m = new CouchbaseMock.HarakiriMonitor(null, server.getLocalPort(), port, false);

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
}
