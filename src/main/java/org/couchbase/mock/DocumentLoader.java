package org.couchbase.mock;

import org.couchbase.mock.client.RestAPIUtil;
import org.couchbase.mock.memcached.protocol.ErrorCode;
import org.couchbase.mock.util.Base64;
import org.couchbase.mock.util.ReaderUtils;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class DocumentLoader {
    static final Pattern ptnDESIGN = Pattern.compile(".*/design_docs/(.*)\\.json$");
    static final Pattern ptnDOCUMENT = Pattern.compile(".*/docs/(.*)\\.json$");

    private final CouchbaseMock mock;
    private final Bucket bucket;

    public DocumentLoader(CouchbaseMock mock, String bucketName) {
        this.mock = mock;
        if (mock != null) {
            Map<String, Bucket> buckets = mock.getBuckets();
            bucket = buckets.get(bucketName);
            if (bucket == null) {
                throw new IllegalArgumentException("No such bucket!");
            }
        } else {
            bucket = null;
        }
    }

    protected void handleDocument(String docId, String contents) {
        ErrorCode result = bucket.storeItem(docId, contents.getBytes(Charset.forName("UTF-8")));

        if (result.value() != ErrorCode.SUCCESS.value()) {
            throw new RuntimeException("Couldn't store: " + result.value());
        }
    }

    protected void handleDesign(String designName, String contents) throws IOException {
        RestAPIUtil.defineDesignDocument(mock, designName, contents, bucket.getName());
    }

    public void loadDocuments(String docsFile) throws IOException {
        ZipFile zipFile = new ZipFile(docsFile);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();

        int numDocs = 0;
        int numDesigns = 0;

        while (entries.hasMoreElements()) {
            ZipEntry ent = entries.nextElement();
            String fName = ent.getName();
            InputStream is = zipFile.getInputStream(ent);
            String contents = ReaderUtils.fromStream(is);

            Matcher mIsDoc = ptnDOCUMENT.matcher(fName);

            if (mIsDoc.matches()) {
                String docId = mIsDoc.group(1);
                handleDocument(docId, contents);
                numDocs++;
                continue;
            }

            Matcher mIsDesign = ptnDESIGN.matcher(fName);
            if (mIsDesign.matches()) {
                String designName = mIsDesign.group(1);
                handleDesign(designName, contents);
                numDesigns++;
            }
        }
        System.err.printf("Loaded %d documents. %d design documents%n", numDocs, numDesigns);
    }

    static class StoredInfo implements Serializable {
        final Map<String,String> designs = new HashMap<String, String>();
        final Map<String,String> documents = new HashMap<String, String>();
    }

    static class BundleSerializer extends DocumentLoader {
        final StoredInfo toStore = new StoredInfo();
        BundleSerializer() {
            super(null, null);
        }

        @Override
        protected void handleDocument(String id, String contents) {
            toStore.documents.put(id, contents);
        }

        @Override
        protected void handleDesign(String id, String contents) {
            toStore.designs.put(id, contents);
        }
    }

    public static void loadFromSerializedXZ(InputStream is, String bucketName, CouchbaseMock mock) throws IOException {
        XZInputStream xzi = new XZInputStream(is);
        ObjectInputStream ois = new ObjectInputStream(xzi);
        StoredInfo si;
        try {
            si = (StoredInfo) ois.readObject();
        } catch (ClassNotFoundException ex) {
            throw new IOException(ex);
        }

        DocumentLoader loader = new DocumentLoader(mock, bucketName);
        for (Map.Entry<String,String> ent : si.documents.entrySet()) {
            loader.handleDocument(ent.getKey(), ent.getValue());
        }
        for (Map.Entry<String,String> ent: si.designs.entrySet()) {
            loader.handleDesign(ent.getKey(), ent.getValue());
        }
        System.err.printf("Finished loading %d documents and %d designs into %s%n", si.documents.size(), si.designs.size(), bucketName);
    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        File outputFile = new File(input.replace(".zip", "") + ".serialized.xz");

        // Get the base name
        FileOutputStream fos = new FileOutputStream(outputFile);
        LZMA2Options options = new LZMA2Options(9);

        XZOutputStream xzo = new XZOutputStream(fos, options);
        ObjectOutputStream oos = new ObjectOutputStream(xzo);

        BundleSerializer ml = new BundleSerializer();
        ml.loadDocuments(input);
        oos.writeObject(ml.toStore);
        oos.flush();
        oos.close();
    }
}