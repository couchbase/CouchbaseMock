package org.couchbase.mock.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public final class ReaderUtils {
    public static String fromStream(InputStream is) throws IOException {
        InputStreamReader isw = new InputStreamReader(is);
        StringBuilder sb = new StringBuilder();
        char buf[] = new char[4096];
        int nRead;
        while ((nRead = isw.read(buf)) > -1) {
            sb.append(buf, 0, nRead);
        }
        return sb.toString();
    }

    public static String fromResource(String path) throws IOException {
        return fromStream(ReaderUtils.class.getClassLoader().getResourceAsStream(path));
    }

    private ReaderUtils() {}
}