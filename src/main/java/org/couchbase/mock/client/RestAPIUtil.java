package org.couchbase.mock.client;

import org.couchbase.mock.Bucket;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.http.Authenticator;
import org.couchbase.mock.util.Base64;
import org.couchbase.mock.util.ReaderUtils;

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
