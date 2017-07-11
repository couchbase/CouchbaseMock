/*
 * Copyright 2017 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.couchbase.mock.http;

import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.util.ReaderUtils;
import junit.framework.TestCase;
import org.apache.http.entity.ContentType;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.couchbase.mock.http.HttpAssert.assertResponseOK;

public class UserManagementHandlerTest extends TestCase {

    private CouchbaseMock mock;

    public UserManagementHandlerTest(String testName) throws IOException {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mock = new CouchbaseMock("localhost", 0, 1, 1);
        mock.start();
    }

    @Override
    protected void tearDown() throws Exception {
        mock.stop();
        super.tearDown();
    }

    public void testListUsers() throws Exception {
        URL url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local");
        assertResponseOK(url);
    }

    public void testUpsertListAndRemoveUser() throws Exception {

        // make sure no users already exist
        assertEquals(0, mock.getUsers().size());

        // create user
        URL url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local/anakin");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write("name=Anakin%20Skywalker&password=secret123&roles=cluster_admin,bucket_admin[default]");
        osw.flush();
        osw.close();
        conn.getInputStream().close();
        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        assertEquals(1, mock.getUsers().size());

        // test list all users
        url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local");
        conn = (HttpURLConnection) url.openConnection();
        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        assertEquals("[{\"id\":\"anakin\",\"name\":\"Anakin Skywalker\",\"domain\":\"local\",\"roles\":[{\"role\":\"bucket_admin\",\"bucketName\":\"default\"}]}]",
                ReaderUtils.fromStream(conn.getInputStream()));
        assertEquals(1, mock.getUsers().size());

        // test get single user
        url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local/anakin");
        conn = (HttpURLConnection) url.openConnection();
        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        assertEquals("{\"id\":\"anakin\",\"name\":\"Anakin Skywalker\",\"domain\":\"local\",\"roles\":[{\"role\":\"bucket_admin\",\"bucketName\":\"default\"}]}",
                ReaderUtils.fromStream(conn.getInputStream()));
        assertEquals(1, mock.getUsers().size());

        // remove user
        url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local/anakin");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("DELETE");
        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        assertEquals(0, mock.getUsers().size());
    }

    public void testMissingPasswordForNewUserReturnsBadRequest() throws Exception {

        // upsert user without password
        URL url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local/anakin");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write("roles=cluster_admin,bucket_admin[default]");
        osw.flush();
        osw.close();

        // throws IO exception because of 400 response
        try {
            conn.getInputStream().close();
        } catch (IOException e) { }

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
        assertEquals(0, mock.getUsers().size());
    }

    public void testMissingPasswordForExistingUserDoesNotReturnBadRequest() throws Exception {

        // add user
        mock.getUsers().put("anakin", new User("local", "anakin"));

        // upsert user without password
        URL url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local/anakin");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write("roles=cluster_admin,bucket_admin[default]");
        osw.flush();
        osw.close();

        // does not throw IO exception because of 400 response
        conn.getInputStream().close();

        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        assertEquals(1, mock.getUsers().size());

        mock.getUsers().clear();
    }

    public void testMissingUsernameReturnsBadRequest() throws Exception {

        // upsert user without username
        URL url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write("password=secret123&roles=cluster_admin,bucket_admin[default]");
        osw.flush();
        osw.close();

        // throws IO exception because of 400 response
        try {
            conn.getInputStream().close();
        } catch (IOException e) { }

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
        assertEquals(0, mock.getUsers().size());
    }

    public void testMissingRolesReturnsBadRequest() throws Exception {
        // upsert user without username
        URL url = new URL("http://localhost:" + mock.getHttpPort() + "/settings/rbac/users/local/anakin");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write("password=secret123");
        osw.flush();
        osw.close();

        // throws IO exception because of 400 response
        try {
            conn.getInputStream().close();
        } catch (IOException e) { }

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
        assertEquals(0, mock.getUsers().size());
    }
}
