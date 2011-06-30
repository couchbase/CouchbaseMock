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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import java.net.Socket;

import junit.framework.TestCase;

import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.http.HttpReasonCode;
import org.couchbase.mock.http.HttpRequestImpl;
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
    Thread thread;


    @Override
    protected void setUp() throws Exception {
        super.setUp();
        instance = new CouchbaseMock(8091, 100, 4096);
        thread = new Thread(instance);
        thread.start();
    }

    @Override
    protected void tearDown() throws Exception {
        thread.interrupt();
        instance.close();
        super.tearDown();
    }

    public void testHandleHttpRequest() throws IOException {
        System.out.println("testHandleHttpRequest");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("GET /pools/default/buckets/default HTTP/1.1");
        pw.println("Authorization: Basic " + Base64.encode("Administrator:password"));
        pw.println();
        pw.flush();

        BufferedReader r = new BufferedReader(new StringReader(sw.toString()));

        HttpRequestImpl request = null;
        try {
            request = new HttpRequestImpl(r);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        instance.handleHttpRequest(request);
        assert (request.getReasonCode() == HttpReasonCode.OK);
    }

    public void testHandleHttpRequestNetwork() throws IOException {
        System.out.println("testHandleHttpRequest");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("GET /pools/default/buckets/default HTTP/1.1");
        pw.println("Authorization: Basic " + Base64.encode("Administrator:password"));
        pw.println();
        pw.flush();

        BufferedReader r = new BufferedReader(new StringReader(sw.toString()));

        Socket s = new Socket("localhost", 8091);
        s.getOutputStream().write(sw.toString().getBytes());
        s.getOutputStream().flush();

        BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
        while (in.readLine() != null) {
            /* Do nothing */
        }
        s.close();
    }

    public void brokenTestHandleHttpRequestMissingAuth() throws IOException {
        System.out.println("testHandleHttpRequestMissingAuth");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("GET /pools/default/buckets/default HTTP/1.1");
        pw.println();
        pw.flush();

        BufferedReader r = new BufferedReader(new StringReader(sw.toString()));

        HttpRequestImpl request = null;
        try {
            request = new HttpRequestImpl(r);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
        instance.handleHttpRequest(request);
        assert (request.getReasonCode() == HttpReasonCode.Unauthorized);
    }

    public void testHandleHttpRequestIncorrectCred() throws IOException {
        System.out.println("testHandleHttpRequestUnkownFile");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("GET /pools/default/buckets/default HTTP/1.1");
        pw.println("Authorization: Basic " + Base64.encode("Bubba:TheHut"));
        pw.println();
        pw.flush();

        BufferedReader r = new BufferedReader(new StringReader(sw.toString()));

        HttpRequestImpl request = null;
        try {
            request = new HttpRequestImpl(r);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
        instance.handleHttpRequest(request);
        assert (request.getReasonCode() == HttpReasonCode.Unauthorized);
    }

    public void testHandleHttpRequestUnkownFile() throws IOException {
        System.out.println("testHandleHttpRequestUnkownFile");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("GET / HTTP/1.1");
        pw.println("Authorization: Basic " + Base64.encode("Administrator:password"));
        pw.println();
        pw.flush();

        BufferedReader r = new BufferedReader(new StringReader(sw.toString()));

        HttpRequestImpl request = null;
        try {
            request = new HttpRequestImpl(r);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
        instance.handleHttpRequest(request);
        assert (request.getReasonCode() == HttpReasonCode.Not_Found);
    }
}
