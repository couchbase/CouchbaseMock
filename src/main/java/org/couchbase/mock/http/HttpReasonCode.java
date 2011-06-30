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
package org.couchbase.mock.http;

/**
 * A mapping between the reason codes and their textual representation.
 *
 * @author Trond Norbye
 */
public enum HttpReasonCode {

    Continue(100), Switching_Protocols(101), OK(200),
    Created(201), Accepted(202), Non_Authoritative_Information(203),
    No_Content(204), Reset_Content(205), Partial_Content(206),
    Multiple_Choices(300), Moved_Permanently(301), Found(302),
    See_Other(303), Not_Modified(304), Use_Proxy(305), Temporary_Redirect(307),
    Bad_Request(400), Unauthorized(401), Payment_Required(402), Forbidden(403),
    Not_Found(404), Method_Not_Allowed(405), Not_Acceptable(406),
    Proxy_Authentication_Required(407), Request_Time_Out(408),
    Conflict(409), Gone(410), Length_Required(411), Precondition_Failed(412),
    Request_Entity_Too_Large(413), Request_URI_Too_Large(414),
    Unsupported_Media_Type(415), Requested_range_not_satisfiable(416),
    Expectation_Failed(417), Internal_Server_Error(500),
    Not_Implemented(501), Bad_Gateway(502), Service_Unavailable(503),
    Gateway_Time_out(504), HTTP_Version_not_supported(505);
    private final int value;

    HttpReasonCode(int value) {
        this.value = value;
    }

    int value() {
        return value;
    }

    static HttpReasonCode valueOf(int cc) {
        switch (cc) {
            case 100:
                return Continue;
            case 101:
                return Switching_Protocols;
            case 200:
                return OK;
            case 201:
                return Created;
            case 202:
                return Accepted;
            case 203:
                return Non_Authoritative_Information;
            case 204:
                return No_Content;
            case 205:
                return Reset_Content;
            case 206:
                return Partial_Content;
            case 300:
                return Multiple_Choices;
            case 301:
                return Moved_Permanently;
            case 302:
                return Found;
            case 303:
                return See_Other;
            case 304:
                return Not_Modified;
            case 305:
                return Use_Proxy;
            case 307:
                return Temporary_Redirect;
            case 400:
                return Bad_Request;
            case 401:
                return Unauthorized;
            case 402:
                return Payment_Required;
            case 403:
                return Forbidden;
            case 404:
                return Not_Found;
            case 405:
                return Method_Not_Allowed;
            case 406:
                return Not_Acceptable;
            case 407:
                return Proxy_Authentication_Required;
            case 408:
                return Request_Time_Out;
            case 409:
                return Conflict;
            case 410:
                return Gone;
            case 411:
                return Length_Required;
            case 412:
                return Precondition_Failed;
            case 413:
                return Request_Entity_Too_Large;
            case 414:
                return Request_URI_Too_Large;
            case 415:
                return Unsupported_Media_Type;
            case 416:
                return Requested_range_not_satisfiable;
            case 417:
                return Expectation_Failed;
            case 500:
                return Internal_Server_Error;
            case 501:
                return Not_Implemented;
            case 502:
                return Bad_Gateway;
            case 503:
                return Service_Unavailable;
            case 504:
                return Gateway_Time_out;
            case 505:
                return HTTP_Version_not_supported;

            default:
                return Internal_Server_Error;
        }
    }

    @Override
    public String toString() {
        return toString(this);
    }

    static String toString(HttpReasonCode cc) {
        switch (cc.value) {
            case 100:
                return "Continue";
            case 101:
                return "Switching Protocols";
            case 200:
                return "OK";
            case 201:
                return "Created";
            case 202:
                return "Accepted";
            case 203:
                return "Non-Authoritative Information";
            case 204:
                return "No Content";
            case 205:
                return "Reset Content";
            case 206:
                return "Partial Content";
            case 300:
                return "Multiple Choices";
            case 301:
                return "Moved Permanently";
            case 302:
                return "Found";
            case 303:
                return "See Other";
            case 304:
                return "Not Modified";
            case 305:
                return "Use Proxy";
            case 307:
                return "Temporary Redirect";
            case 400:
                return "Bad Request";
            case 401:
                return "Unauthorized";
            case 402:
                return "Payment Required";
            case 403:
                return "Forbidden";
            case 404:
                return "Not Found";
            case 405:
                return "Method Not Allowed";
            case 406:
                return "Not Acceptable";
            case 407:
                return "Proxy Authentication Required";
            case 408:
                return "Request Time-out";
            case 409:
                return "Conflict";
            case 410:
                return "Gone";
            case 411:
                return "Length Required";
            case 412:
                return "Precondition Failed";
            case 413:
                return "Request Entity Too Large";
            case 414:
                return "Request-URI Too Large";
            case 415:
                return "Unsupported Media Type";
            case 416:
                return "Requested range not satisfiable";
            case 417:
                return "Expectation Failed";
            case 500:
                return "Internal Server Error";
            case 501:
                return "Not Implemented";
            case 502:
                return "Bad Gateway";
            case 503:
                return "Service Unavailable";
            case 504:
                return "Gateway Time-out";
            case 505:
                return "HTTP Version not supported";
            default:
                return "Internal Server Error";
        }
    }
}
