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
import org.couchbase.mock.memcached.protocol.ComCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

/**
 *
 * @author Trond Norbye <trond.norbye@gmail.com>
 */
public class ArithmeticCommandExecutor implements CommandExecutor {

    @Override
    public void execute(BinaryCommand command, MemcachedServer server, MemcachedConnection client) {
        BinaryArithmeticCommand cmd = (BinaryArithmeticCommand) command;
        Item item = server.getDatastore().get(server, cmd.getVBucketId(), cmd.getKey());
        ComCode cc = cmd.getComCode();

        if (item == null) {
            if (cmd.create()) {
                item = new Item(cmd.getKey(), 0, cmd.getExpiration(), Long.toString(cmd.getInitial()).getBytes(), 0);
                ErrorCode err = server.getDatastore().add(server, cmd.getVBucketId(), item);

                switch (err) {
                    case KEY_EEXISTS:
                        execute(command, server, client);
                        break;
                    case SUCCESS:
                        if (cc == ComCode.INCREMENT || cc == ComCode.DECREMENT) {
                            client.sendResponse(new BinaryArithmeticResponse(cmd, cmd.getInitial(), item.getCas()));
                        }
                        break;
                    default:
                        client.sendResponse(new BinaryResponse(command, err));
                }
            } else {
                client.sendResponse(new BinaryResponse(command, ErrorCode.KEY_ENOENT));
            }
            return;
        } else {
            long value;
            try {
                value = Long.parseLong(new String(item.getValue()));
            } catch (NumberFormatException ex) {
                client.sendResponse(new BinaryResponse(command, ErrorCode.DELTA_BADVAL));
                return;
            }

            if (cc == ComCode.INCREMENT || cc == ComCode.INCREMENTQ) {
                value += cmd.getDelta();
            } else {
                value -= cmd.getDelta();
            }

            int exp = cmd.getExpiration() > 0 ? cmd.getExpiration() : item.getExptime();
            Item nval = new Item(cmd.getKey(), 0, exp, Long.toString(value).getBytes(), item.getCas());
            ErrorCode err = server.getDatastore().set(server, cmd.getVBucketId(), nval);
            if (err == ErrorCode.SUCCESS) {
                if (cc == ComCode.INCREMENT || cc == ComCode.DECREMENT) {
                    // return value
                    client.sendResponse(new BinaryArithmeticResponse(cmd, value, nval.getCas()));
                }
            } else {
                client.sendResponse(new BinaryResponse(command, err));
            }
        }
    }
}
