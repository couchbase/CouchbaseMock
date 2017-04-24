/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.client;

import java.util.ArrayList;
import java.util.List;

import com.sun.corba.se.spi.orb.OperationFactory;
import org.couchbase.mock.memcached.protocol.ErrorCode;

public class OpfailRequest extends MockRequest {
    public OpfailRequest(ErrorCode code, int count, List<Integer> servers) {
        super();
        setName("opfail");
        payload.put("code", code.value());
        payload.put("count", count);
        if (servers != null) {
            payload.put("servers", servers);
        }
    }

    public OpfailRequest(ErrorCode code, int count) {
        this(code, count, null);
    }

    private static List<Integer> singleServerList(int index) {
        List<Integer> ret = new ArrayList<Integer>();
        ret.add(index);
        return ret;
    }

    public OpfailRequest(ErrorCode code, int count, int server) {
        this(code, count, singleServerList(server));
    }
}