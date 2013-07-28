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

import junit.framework.TestCase;

/**
 * Test that the utility functions in Base64 works as expected.
 *
 * @author Trond Norbye
 */
@SuppressWarnings("SpellCheckingInspection")
public class Base64Test extends TestCase {

    private void validateEncode(String input, String expResult) {
        String result = Base64.encode(input);
        assertEquals(expResult, result);
    }

    /**
     * Test of encode method, of class Base64.
     */
    public void testEncode() {
        validateEncode("Aladdin:open sesame", "QWxhZGRpbjpvcGVuIHNlc2FtZQ==");

        /* Test cases from RFC 4648 */
        validateEncode("", "");
        validateEncode("f", "Zg==");
        validateEncode("fo", "Zm8=");
        validateEncode("foo", "Zm9v");
        validateEncode("foob", "Zm9vYg==");
        validateEncode("fooba", "Zm9vYmE=");
        validateEncode("foobar", "Zm9vYmFy");

        /* Examples from http://en.wikipedia.org/wiki/Base64 */
        validateEncode("Man is distinguished, not only by his reason, but by this singular "
                + "passion from other animals, which is a lust of the mind, that by a "
                + "perseverance of delight in the continued and indefatigable generation"
                + " of knowledge, exceeds the short vehemence of any carnal pleasure.",
                "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0aGlz"
                + "IHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2Yg"
                + "dGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0aGUgY29udGlu"
                + "dWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdlLCBleGNlZWRzIHRo"
                + "ZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=");
        validateEncode("pleasure.", "cGxlYXN1cmUu");
        validateEncode("leasure.", "bGVhc3VyZS4=");
        validateEncode("easure.", "ZWFzdXJlLg==");
        validateEncode("asure.", "YXN1cmUu");
        validateEncode("sure.", "c3VyZS4=");

        /* Dummy test data  It looks like the "base64" command line utility from gnu
         * coreutils adds the "\n" to the encoded data...
         */
        validateEncode("Administrator:password", "QWRtaW5pc3RyYXRvcjpwYXNzd29yZA==");
        validateEncode("@", "QA==");
        validateEncode("@\n", "QAo=");
        validateEncode("@@", "QEA=");
        validateEncode("@@\n", "QEAK");
        validateEncode("@@@", "QEBA");
        validateEncode("@@@\n", "QEBACg==");
        validateEncode("@@@@", "QEBAQA==");
        validateEncode("@@@@\n", "QEBAQAo=");
        validateEncode("blahblah:bla@@h", "YmxhaGJsYWg6YmxhQEBo");
        validateEncode("blahblah:bla@@h\n", "YmxhaGJsYWg6YmxhQEBoCg==");
    }

    private void validateDecode(String input, String expResult) {
        String result = Base64.decode(input);
        assertEquals(expResult, result);
    }

    /**
     * Test of decode method, of class Base64.
     */
    public void testDecode() {
        validateDecode("QWxhZGRpbjpvcGVuIHNlc2FtZQ==", "Aladdin:open sesame");

        /* Test cases from RFC 4648 */
        validateDecode("", "");
        validateDecode("Zg==", "f");
        validateDecode("Zm8=", "fo");
        validateDecode("Zm9v", "foo");
        validateDecode("Zm9vYg==", "foob");
        validateDecode("Zm9vYmE=", "fooba");
        validateDecode("Zm9vYmFy", "foobar");

        /* Examples from http://en.wikipedia.org/wiki/Base64 */
        validateDecode("TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0aGlz"
                + "IHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2Yg"
                + "dGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0aGUgY29udGlu"
                + "dWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdlLCBleGNlZWRzIHRo"
                + "ZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=",
                "Man is distinguished, not only by his reason, but by this singular "
                + "passion from other animals, which is a lust of the mind, that by a "
                + "perseverance of delight in the continued and indefatigable generation"
                + " of knowledge, exceeds the short vehemence of any carnal pleasure.");
        validateDecode("cGxlYXN1cmUu", "pleasure.");
        validateDecode("bGVhc3VyZS4=", "leasure.");
        validateDecode("ZWFzdXJlLg==", "easure.");
        validateDecode("YXN1cmUu", "asure.");
        validateDecode("c3VyZS4=", "sure.");

        /* Dummy test data  It looks like the "base64" command line utility from gnu
         * coreutils adds the "\n" to the encoded data...
         */
        validateDecode("QWRtaW5pc3RyYXRvcjpwYXNzd29yZA==", "Administrator:password");
        validateDecode("QA==", "@");
        validateDecode("QAo=", "@\n");
        validateDecode("QEA=", "@@");
        validateDecode("QEAK", "@@\n");
        validateDecode("QEBA", "@@@");
        validateDecode("QEBACg==", "@@@\n");
        validateDecode("QEBAQA==", "@@@@");
        validateDecode("QEBAQAo=", "@@@@\n");
        validateDecode("YmxhaGJsYWg6YmxhQEBo", "blahblah:bla@@h");
        validateDecode("YmxhaGJsYWg6YmxhQEBoCg==", "blahblah:bla@@h\n");
        validateDecode("QWxhZGRpbjpvcGVuIHNlc2FtZQ==", "Aladdin:open sesame");
    }
}
