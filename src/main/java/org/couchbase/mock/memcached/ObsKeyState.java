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
package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.protocol.ObserveCode;

/**
 *
 * @author Mark Nunberg
 */

public class ObsKeyState extends KeySpec {
    public final ObserveCode status;
    public final long cas;

    public ObsKeyState(KeySpec ks, ObserveCode status, long cas) {
        super(ks.key, ks.vbId);
        this.status = status;
        this.cas = cas;
    }

    public ObsKeyState(Item itm, ObserveCode status) {
        super(itm.getKeySpec().key, itm.getKeySpec().vbId);
        this.status = status;
        this.cas = itm.getCas();
    }
}
