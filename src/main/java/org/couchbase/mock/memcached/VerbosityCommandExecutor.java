/*
 * Copyright 2011 Couchbase, Inc.
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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * Implementation of the VERBOSITY command
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
class VerbosityCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        int req = cmd.getInputBuffer().getInt(0);
        Level level;

        switch (req) {
            case 0:
                level = Level.OFF;
                break;
            case 1:
                level = Level.SEVERE;
                break;
            case 2:
                level = Level.INFO;
                break;
            default:
                level = Level.FINEST;
        }

        Logger.getLogger("org.couchbase.mock").setLevel(level);
        client.sendResponse(new BinaryResponse(cmd, ErrorCode.SUCCESS));
    }
}
