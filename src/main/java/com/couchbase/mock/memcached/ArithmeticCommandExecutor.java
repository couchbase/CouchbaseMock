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

import com.couchbase.mock.memcached.protocol.BinaryArithmeticCommand;
import com.couchbase.mock.memcached.protocol.BinaryArithmeticResponse;
import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.Datatype;
import com.couchbase.mock.memcached.protocol.ErrorCode;

import java.net.ProtocolException;

/**
 * @author Trond Norbye
 */
public class ArithmeticCommandExecutor implements CommandExecutor {

    @Override
    public BinaryResponse execute(BinaryCommand command, MemcachedServer server, MemcachedConnection client) throws ProtocolException {
        BinaryArithmeticCommand cmd = (BinaryArithmeticCommand) command;
        VBucketStore cache = server.getStorage().getCache(server, cmd.getVBucketId());
        Item item = cache.get(cmd.getKeySpec());
        CommandCode cc = cmd.getComCode();
        MutationInfoWriter miw = client.getMutinfoWriter();

        if (item == null) {
            if (cmd.create()) {
                item = new Item(cmd.getKeySpec(), 0, cmd.getExpiration(), Long.toString(cmd.getInitial()).getBytes(), null, 0, Datatype.RAW.value());
                MutationStatus ms = cache.add(item, client.supportsXerror());
                ErrorCode err = ms.getStatus();

                switch (err) {
                    case KEY_EEXISTS:
                        return execute(command, server, client);
                    case SUCCESS:
                        if (cc == CommandCode.INCREMENT || cc == CommandCode.DECREMENT) {
                            return new BinaryArithmeticResponse(cmd, cmd.getInitial(), item.getCas(), ms, miw);
                        } else {
                            throw new ProtocolException("invalid opcode for Arithmetic handler: " + cc);
                        }
                    default:
                        return new BinaryResponse(command, err);
                }
            } else {
                return new BinaryResponse(command, ErrorCode.KEY_ENOENT);
            }
        } else {
            long value;

            if (!item.ensureUnlocked(command.getCas())) {
                return new BinaryResponse(command, ErrorCode.ETMPFAIL);
            }

            try {
                value = Long.parseLong(new String(item.getValue()));
            } catch (NumberFormatException ex) {
                return new BinaryResponse(command, ErrorCode.DELTA_BADVAL);
            }

            if (cc == CommandCode.INCREMENT || cc == CommandCode.INCREMENTQ) {
                value += cmd.getDelta();
            } else {
                value -= cmd.getDelta();
            }

            int exp = cmd.getExpiration() > 0 ? cmd.getExpiration() : item.getExpiryTime();
            Item newValue = new Item(cmd.getKeySpec(), item.getFlags(), exp, Long.toString(value).getBytes(), null, item.getCas(), Datatype.RAW.value());
            MutationStatus ms = cache.set(newValue, client.supportsXerror());
            if (ms.getStatus() == ErrorCode.SUCCESS) {
                if (cc == CommandCode.INCREMENT || cc == CommandCode.DECREMENT) {
                    return new BinaryArithmeticResponse(cmd, value, newValue.getCas(), ms, miw);
                } else {
                    throw new ProtocolException("invalid opcode for Arithmetic handler: " + cc);
                }
            } else {
                return new BinaryResponse(command, ms.getStatus());
            }
        }
    }
}
