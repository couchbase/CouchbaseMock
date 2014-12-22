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
package org.couchbase.mock.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to perform base64 encoding / decoding
 *
 * @author Trond Norbye
 */
public class Base64 {

    @SuppressWarnings("SpellCheckingInspection")
    private static final char code[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();

    /**
     * Private constructor to avoid users create instances of the class
     */
    private Base64() {
    }

    private static void encodeRest(StringBuilder out, byte[] s, int num) {
        long val;
        if (num == 2) {
            val = ((s[0] << 16) | (s[1] << 8));
        } else {
            val = ((s[0] << 16));
        }

        out.append(code[(int) ((val >> 18) & 63)]);
        out.append(code[(int) ((val >> 12) & 63)]);

        if (num == 2) {
            out.append(code[(int) ((val >> 6) & 63)]);
        } else {
            out.append('=');
        }

        out.append('=');
    }

    private static void encodeTriplet(StringBuilder out, byte[] s) {
        int val = ((s[0] << 16) | ((s[1]) << 8) | s[2]);
        out.append(code[(val >> 18) & 63]);
        out.append(code[(val >> 12) & 63]);
        out.append(code[(val >> 6) & 63]);
        out.append(code[(val & 63)]);
    }

    @SuppressWarnings("fallthrough")
    private static boolean encode(StringBuilder out, InputStream in) throws IOException {
        byte s[] = new byte[3];
        int num = in.read(s);

        switch (num) {
            case 3:
                encodeTriplet(out, s);
                return true;
            case 2:
            case 1:
                encodeRest(out, s, num);
                // FALLTHROUGH
            case -1:
                return false;
            default:
                throw new AssertionError("Invalid length! " + num);
        }
    }

    /**
     * Base64 encode a textual string according to RFC 3548
     *
     * @param input The bytes to encode
     * @return The encoded string
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public static String encode(byte[] input) {
        ByteArrayInputStream in = new ByteArrayInputStream(input);
        StringBuilder sb = new StringBuilder();
        try {
            while (encode(sb, in)) {
                // do nothing
            }
        } catch (IOException ex) {
            Logger.getLogger(Base64.class.getName()).log(Level.SEVERE, null, ex);
        }


        return sb.toString();
    }

    public static String encode(String input) {
        return encode(input.getBytes());
    }

    private static byte getByte(byte val) {
        for (byte ii = 0; ii < code.length; ++ii) {
            if (code[ii] == val) {
                return ii;
            }
        }
        throw new IllegalAccessError();
    }

    private static boolean decode(StringBuilder out, InputStream in) throws IOException {
        byte s[] = new byte[4];
        int num = in.read(s);
        if (num == -1) {
            return false;
        }
        int len = 3;

        int val = 0;

        val |= (getByte(s[0]) << 18);
        val |= (getByte(s[1]) << 12);
        if (s[2] == '=') {
            --len;
        } else {
            val |= (getByte(s[2]) << 6);
        }
        if (s[3] == '=') {
            --len;
        } else {
            val |= getByte(s[3]);
        }

        out.append((char) ((val >> 16) & 0xff));
        if (len > 1) {
            out.append((char) ((val >> 8) & 0xff));
        }
        if (len > 2) {
            out.append((char) (val & 0xff));
        }

        return len == 3;
    }

    /**
     * Decode a Base64 encoded string
     *
     * @param input The string to decode
     * @return The decoded string
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public static String decode(String input) {
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
        StringBuilder sb = new StringBuilder();
        try {
            while (decode(sb, in)) {
                // do nothing
            }
        } catch (IOException ex) {
            Logger.getLogger(Base64.class.getName()).log(Level.SEVERE, null, ex);
        }


        return sb.toString();
    }
}
