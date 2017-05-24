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

package com.couchbase.mock.client;

import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.Bucket;
import com.couchbase.mock.http.Authenticator;
import com.couchbase.mock.util.Base64;
import com.couchbase.mock.util.ReaderUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class RestAPIUtil {
    private static URL getDesignURL(CouchbaseMock mock, String designName, String bucketName) throws MalformedURLException {
        return new URL(String.format("http://%s:%d/%s/_design/%s",
                mock.getHttpHost(), mock.getHttpPort(), bucketName, designName));
    }

    private static void setAuthHeaders(CouchbaseMock mock, String bucketName, HttpURLConnection conn) {
        Bucket bucket = mock.getBuckets().get(bucketName);
        if (!bucket.getPassword().isEmpty()) {
            String authStr = "Basic " + Base64.encode(bucket.getName() + ":" + bucket.getPassword());
            conn.setRequestProperty("Authorization", authStr);
        }
    }

    public static void setAdminHeader(CouchbaseMock mock, HttpURLConnection conn) {
        Authenticator ac = mock.getAuthenticator();
        String authStr = "Basic " + Base64.encode(ac.getAdminName() + ":" + ac.getAdminPass());
        conn.setRequestProperty("Authorization", authStr);
    }

    // Utility method to define a view
    public static void defineDesignDocument(CouchbaseMock mock, String designName, String contents, String bucketName) throws IOException {
        URL url = getDesignURL(mock, designName, bucketName);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        setAuthHeaders(mock, bucketName, conn);

        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.setDoInput(true);

        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write(contents);
        osw.flush();
        osw.close();

        try {
            conn.getInputStream().close();
        } catch (IOException ex) {
            InputStream es = conn.getErrorStream();
            if (es != null) {
                System.err.printf("Problem creating view: %s%n", ReaderUtils.fromStream(es));
            } else {
                System.err.printf("Error stream is null!\n");
            }
            throw ex;
        }
    }

    public static void deleteDeignDocument(CouchbaseMock mock, String designName, String bucketName) throws IOException {
        URL url = getDesignURL(mock, designName, bucketName);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        setAuthHeaders(mock, bucketName, conn);
        conn.setRequestMethod("DELETE");
        ReaderUtils.fromStream(conn.getInputStream());
        conn.getInputStream().close();
    }

    public static void loadBeerSample(CouchbaseMock mock) throws IOException {
        URL url = new URL(String.format("http://%s:%d/sampleBuckets/install", mock.getHttpHost(), mock.getHttpPort()));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        setAdminHeader(mock, conn);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        String data = "[\"beer-sample\"]";
        OutputStreamWriter osw = new OutputStreamWriter(conn.getOutputStream());
        osw.write(data);
        osw.flush();
        osw.close();
        conn.getInputStream().close();
    }
}
