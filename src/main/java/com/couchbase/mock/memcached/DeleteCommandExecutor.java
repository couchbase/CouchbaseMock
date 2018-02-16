/*
 * Copyright 2017 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.couchbase.mock.memcached;

import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.ErrorCode;

import java.net.ProtocolException;

/**
 * @author Trond Norbye
 */
public class DeleteCommandExecutor implements CommandExecutor {

    @Override
    public BinaryResponse execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) throws ProtocolException {
        VBucketStore cache = server.getStorage().getCache(server, cmd.getVBucketId());
        MutationStatus ms = cache.delete(cmd.getKeySpec(), cmd.getCas(), client.supportsXerror());
        ErrorCode err = ms.getStatus();

        if (!(cmd.getComCode() == CommandCode.DELETEQ && err == ErrorCode.SUCCESS)) {
            return new BinaryResponse(cmd, ms, client.getMutinfoWriter(), 0);
        } else {
            throw new ProtocolException("invalid opcode for Delete handler: " + cmd.getComCode());
        }
    }
}
