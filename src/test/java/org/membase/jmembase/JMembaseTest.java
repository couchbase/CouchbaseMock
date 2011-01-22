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
package org.membase.jmembase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import junit.framework.TestCase;
import org.membase.jmembase.http.HttpReasonCode;
import org.membase.jmembase.http.HttpRequestImpl;
import org.membase.jmembase.util.Base64;

/**
 * Basic testing of JMembase
 * 
 * @author Trond Norbye
 */
public class JMembaseTest extends TestCase {

    public JMembaseTest(String testName) {
        super(testName);
    }
    JMembase instance;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        instance = new JMembase(8091, 100, 4096);
    }

    @Override
    protected void tearDown() throws Exception {
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

    public void testHandleHttpRequestMissingAuth() throws IOException {
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
        System.out.println("htestHandleHttpRequestUnkownFile");
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
