/*
 * Copyright 2015 Couchbase, Inc.
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

package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.SubdocCommandExecutor.ResultInfo;
import org.couchbase.mock.memcached.protocol.*;
import org.couchbase.mock.subdoc.Operation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SubdocMultiCommandExecutor implements CommandExecutor {
    static class SpecResult {
        final int index;
        final ErrorCode ec;
        final String value;
        SpecResult(int index, ErrorCode ec) {
            this.index = index;
            this.ec = ec;
            this.value = null;
        }
        SpecResult(int index, String value) {
            this.index = index;
            this.value = value;
            this.ec = ErrorCode.SUCCESS;
        }
    }

    static class ExecutorContext {
        final List<SpecResult> results;
        final List<BinarySubdocMultiCommand.MultiSpec> specs;
        final BinarySubdocMultiCommand command;
        final MemcachedConnection client;
        final Item existing;
        final VBucketStore cache;
        String currentDoc;

        boolean isMutator() {
            return command.getComCode() == CommandCode.SUBDOC_MULTI_MUTATION;
        }

        ExecutorContext(BinaryCommand cmd, MemcachedConnection client, Item existing, VBucketStore cache) {
            this.existing = existing;
            currentDoc = new String(existing.getValue());
            this.command = (BinarySubdocMultiCommand)cmd;
            this.client = client;
            this.specs = command.getLookupSpecs();
            this.cache = cache;
            results = new ArrayList<SpecResult>();
        }

        private boolean handleLookupSpec(BinarySubdocMultiCommand.MultiSpec spec, int index) {
            Operation op = BinarySubdocCommand.toSubdocOpcode(spec.getOp());
            if (op == null) {
                results.add(new SpecResult(index, ErrorCode.UNKNOWN_COMMAND));
                return true;
            }
            if (!op.isLookup()) {
                client.sendResponse(new BinaryResponse(command, ErrorCode.SUBDOC_INVALID_COMBO));
                return false;
            }
            ResultInfo rsi = SubdocCommandExecutor.executeSubdocLookup(op, currentDoc, spec.getPath());
            switch (rsi.getStatus()) {
                case SUCCESS:
                    if (op.returnsMatch()) {
                        results.add(new SpecResult(index, rsi.getMatchString()));
                    } else {
                        results.add(new SpecResult(index, ErrorCode.SUCCESS));
                    }
                    return true;
                case SUBDOC_DOC_NOTJSON:
                case SUBDOC_DOC_E2DEEP:
                    client.sendResponse(new BinaryResponse(command, rsi.getStatus()));
                    return false;
                default:
                    results.add(new SpecResult(index, rsi.getStatus()));
                    return true;
            }
        }

        private boolean sendMutationError(ErrorCode ec, int index) {
            ByteBuffer bb = ByteBuffer.allocate(3);
            bb.put((byte)index);
            bb.putShort(ec.value());
            ErrorCode topLevelRc;
            if (ec == ErrorCode.SUBDOC_INVALID_COMBO) {
                topLevelRc = ec;
            } else {
                topLevelRc = ErrorCode.SUBDOC_MULTI_FAILURE;
            }
            BinaryResponse br = BinaryResponse.createWithValue(topLevelRc, command, bb.array(), 0);
            client.sendResponse(br);
            return false;
        }

        private boolean handleMutationSpec(BinarySubdocMultiCommand.MultiSpec spec, int index) {
            Operation op = BinarySubdocCommand.toSubdocOpcode(spec.getOp());

            if (op == null) {
                return sendMutationError(ErrorCode.UNKNOWN_COMMAND, index);
            }

            if (!op.isMutator()) {
                return sendMutationError(ErrorCode.SUBDOC_INVALID_COMBO, index);
            }

            ResultInfo rsi = SubdocCommandExecutor.executeSubdocOperation(
                    op, currentDoc, spec.getPath(), spec.getValue(), spec.getFlags());
            if (rsi.getStatus() != ErrorCode.SUCCESS) {
                return sendMutationError(rsi.getStatus(), index);
            }

            if (op.returnsMatch()) {
                results.add(new SpecResult(index, rsi.getMatchString()));
            }

            currentDoc = rsi.getNewDocString();
            return true;
        }

        void execute() {
            for (int i = 0; i < specs.size(); i++) {
                BinarySubdocMultiCommand.MultiSpec spec = specs.get(i);
                boolean result;
                if (isMutator()) {
                    result = handleMutationSpec(spec, i);
                } else {
                    result = handleLookupSpec(spec, i);
                }
                if (!result) {
                    // Assume response was sent.
                    return;
                }
            }

            if (isMutator()) {
                MutationInfoWriter miw = client.getMutinfoWriter();
                Item newItem = new Item(
                        existing.getKeySpec(),
                        existing.getFlags(),
                        command.getNewExpiry(existing.getExpiryTime()),
                        currentDoc.getBytes(),
                        command.getCas());
                MutationStatus ms = cache.replace(newItem);

                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                for (SpecResult result : results) {
                    int specLen = 3;

                    if (result.ec == ErrorCode.SUCCESS) {
                        specLen += 4;
                        specLen += result.value.length();
                    }

                    ByteBuffer bb = ByteBuffer.allocate(specLen);
                    bb.put((byte)result.index);
                    bb.putShort(result.ec.value());
                    if (result.ec == ErrorCode.SUCCESS) {
                        bb.putInt(result.value.length());
                        bb.put(result.value.getBytes());
                    }
                    try {
                        bao.write(bb.array());
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                client.sendResponse(new BinaryResponse(command, ms, miw, newItem.getCas(), bao.toByteArray()));
            } else {
                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                boolean hasError = false;
                for (SpecResult result : results) {
                    String value = result.value;

                    if (value == null) {
                        value = "";
                    }

                    ByteBuffer bb = ByteBuffer.allocate(6 + value.length());
                    bb.putShort(result.ec.value());
                    bb.putInt(value.length());
                    bb.put(value.getBytes());
                    if (result.ec != ErrorCode.SUCCESS) {
                        hasError = true;
                    }
                    try {
                        bao.write(bb.array());
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                byte[] multiPayload  = bao.toByteArray();
                ErrorCode finalEc = hasError ? ErrorCode.SUBDOC_MULTI_FAILURE : ErrorCode.SUCCESS;
                BinaryResponse resp = BinaryResponse.createWithValue(finalEc, command, multiPayload, existing.getCas());
                client.sendResponse(resp);
            }
        }
    }

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        VBucketStore cache = server.getCache(cmd);
        Item existing = cache.get(cmd.getKeySpec());

        if (existing == null) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.KEY_ENOENT));
            return;
        }

        ExecutorContext cx = new ExecutorContext(cmd, client, existing, cache);
        cx.execute();
    }

}
