/*
 *  Copyright 2011 Couchbase, Inc..
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.protocol.BinaryArithmeticCommand;
import org.couchbase.mock.memcached.protocol.BinaryArithmeticResponse;
import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryResponse;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class ArithmeticCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand command, MemcachedServer server, MemcachedConnection client) {
        BinaryArithmeticCommand cmd = (BinaryArithmeticCommand) command;
        VBucketStore cache = server.getStorage().getCache(server, cmd.getVBucketId());
        Item item = cache.get(cmd.getKeySpec());
        CommandCode cc = cmd.getComCode();
        MutationInfoWriter miw = client.getMutinfoWriter();

        if (item == null) {
            if (cmd.create()) {
                item = new Item(cmd.getKeySpec(), 0, cmd.getExpiration(), Long.toString(cmd.getInitial()).getBytes(), 0);
                MutationStatus ms = cache.add(item);
                ErrorCode err = ms.getStatus();

                switch (err) {
                    case KEY_EEXISTS:
                        execute(command, server, client);
                        break;
                    case SUCCESS:
                        if (cc == CommandCode.INCREMENT || cc == CommandCode.DECREMENT) {
                            client.sendResponse(new BinaryArithmeticResponse(cmd, cmd.getInitial(), item.getCas(), ms, miw));
                        }
                        break;
                    default:
                        client.sendResponse(new BinaryResponse(command, err));
                }
            } else {
                client.sendResponse(new BinaryResponse(command, ErrorCode.KEY_ENOENT));
            }
        } else {
            long value;

            if (!item.ensureUnlocked(command.getCas())) {
                client.sendResponse(new BinaryResponse(command, ErrorCode.ETMPFAIL));
                return;
            }

            try {
                value = Long.parseLong(new String(item.getValue()));
            } catch (NumberFormatException ex) {
                client.sendResponse(new BinaryResponse(command, ErrorCode.DELTA_BADVAL));
                return;
            }

            if (cc == CommandCode.INCREMENT || cc == CommandCode.INCREMENTQ) {
                value += cmd.getDelta();
            } else {
                value -= cmd.getDelta();
            }

            int exp = cmd.getExpiration() > 0 ? cmd.getExpiration() : item.getExpiryTime();
            Item newValue = new Item(cmd.getKeySpec(), item.getFlags(), exp, Long.toString(value).getBytes(), item.getCas());
            MutationStatus ms = cache.set(newValue);
            if (ms.getStatus() == ErrorCode.SUCCESS) {
                if (cc == CommandCode.INCREMENT || cc == CommandCode.DECREMENT) {
                    // return value
                    client.sendResponse(new BinaryArithmeticResponse(cmd, value, newValue.getCas(), ms, miw));
                }
            } else {
                client.sendResponse(new BinaryResponse(command, ms.getStatus()));
            }
        }
    }
}
