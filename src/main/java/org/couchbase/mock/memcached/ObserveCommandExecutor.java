/*
 * Copyright 2013 Couchbase.
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

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryObserveCommand;
import org.couchbase.mock.memcached.protocol.BinaryObserveResponse;
import org.couchbase.mock.memcached.protocol.ObserveCode;

/**
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public class ObserveCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        BinaryObserveCommand command = (BinaryObserveCommand)cmd;
        Storage storage = server.getStorage();
        List<ObsKeyState> states = new ArrayList<ObsKeyState>();

        for (KeySpec spec : command.getKeySpecs()) {
            VBucketInfo vbInfo = storage.getVBucketInfo(spec.vbId);
            if (!vbInfo.hasAccess(server)) {
                throw new AccessControlException("not my vbucket");
            }

            Item cached = storage.getCached(spec);
            Item persisted = storage.getPersisted(spec);
            ObserveCode code;
            ObsKeyState kState;

            if (cached == null) {
                /**
                 * Not in cache. It's either not existing at all, or has
                 * not yet been removed from persistent store
                 */
                if (persisted == null) {
                    code = ObserveCode.NOT_FOUND;

                } else {
                    code = ObserveCode.LOGICALLY_DELETED;
                }
                kState = new ObsKeyState(spec, code, 0);

            } else {
                /** Exists in cache */

                if (persisted == null) {
                    code = ObserveCode.NOT_PERSISTED;

                } else {
                    /** Check versions */
                    if (persisted.getCas() != cached.getCas()) {
                        code = ObserveCode.NOT_PERSISTED;

                    } else {
                        code = ObserveCode.PERSISTED;
                    }
                }
                kState = new ObsKeyState(cached, code);
            }

            states.add(kState);
        }

        client.sendResponse(new BinaryObserveResponse(cmd, states));
    }
}
