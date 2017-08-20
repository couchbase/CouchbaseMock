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

import com.couchbase.mock.util.Base64;

import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class HttpAssert {
    public static void assertResponseOK(URL url, String username, String password) {
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            if (username != null && password != null) {
                conn.addRequestProperty("Authorization", "Basic " + Base64.encode(String.format("%s:%s", username, password)));
            }
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public static void assertResponseOK(URL url) {
        assertResponseOK(url, null, null);
    }

    public static void assertResponseUnauthorized(URL url, String username, String password) {
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            if (username != null && password != null) {
                conn.addRequestProperty("Authorization", "Basic " + Base64.encode(String.format("%s:%s", username, password)));
            }
            assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public static void assertResponseUnauthorized(URL url) {
        assertResponseUnauthorized(url, null, null);
    }

    public static void assertResponseNotFound(URL url, String username, String password) {
        HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) url.openConnection();
            assertNotNull(conn);
            conn.addRequestProperty("Authorization", "Basic " + Base64.encode(String.format("%s:%s", username, password)));
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }
}
