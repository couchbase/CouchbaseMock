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

import com.couchbase.mock.memcached.SubdocCommandExecutor.ResultInfo;
import com.couchbase.mock.memcached.protocol.BinaryCommand;
import com.couchbase.mock.memcached.protocol.BinaryResponse;
import com.couchbase.mock.memcached.protocol.BinarySubdocCommand;
import com.couchbase.mock.memcached.protocol.BinarySubdocMultiCommand;
import com.couchbase.mock.memcached.protocol.BinarySubdocMultiMutationCommand;
import com.couchbase.mock.memcached.protocol.CommandCode;
import com.couchbase.mock.memcached.protocol.Datatype;
import com.couchbase.mock.memcached.protocol.ErrorCode;
import com.couchbase.mock.subdoc.Operation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SubdocMultiCommandExecutor implements CommandExecutor {
    @Override
    public BinaryResponse execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        VBucketStore cache = server.getCache(cmd);
        Item existing = cache.get(cmd.getKeySpec());
        ExecutorContext cx;

        if (existing == null) {
            // Not a mutation. No point in making fake documents
            if (!(cmd instanceof BinarySubdocMultiMutationCommand)) {
                return new BinaryResponse(cmd, ErrorCode.KEY_ENOENT);
            }

            BinarySubdocMultiMutationCommand mcmd = (BinarySubdocMultiMutationCommand) cmd;
            if ((mcmd.getSubdocDocFlags() & (BinarySubdocCommand.DOCFLAG_CREATEMASK)) == 0) {
                return new BinaryResponse(cmd, ErrorCode.KEY_ENOENT);
            }

            String rootString = mcmd.getRootType();
            if (rootString == null) {
                return new BinaryResponse(cmd, ErrorCode.KEY_ENOENT);
            }

            Item newItem = new Item(cmd.getKeySpec(), 0, 0, rootString.getBytes(), "{}".getBytes(), 0, Datatype.RAW.value());
            cx = new ExecutorContext(cmd, client, newItem, cache, true);

        } else {
            if (cmd instanceof BinarySubdocMultiMutationCommand) {
                if ((((BinarySubdocMultiMutationCommand) cmd).getSubdocDocFlags() & BinarySubdocCommand.DOCFLAG_ADD) != 0) {
                    return new BinaryResponse(cmd, ErrorCode.KEY_EEXISTS);
                }
            }
            cx = new ExecutorContext(cmd, client, existing, cache, false);
        }

        return cx.execute();
    }

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

        // True if we've encountered at least *ONE* extended attribute in the spec to create.
        // This tells us whether if the xattribute is simply "{}" to write it or not.
        boolean hasXattrSpec;
        boolean needCreate;
        String currentDoc;
        String currentAttrs;

        ExecutorContext(
                BinaryCommand cmd, MemcachedConnection client, Item existing, VBucketStore cache, boolean needCreate) {
            this.existing = existing;
            currentDoc = new String(existing.getValue());
            currentAttrs = new String(existing.getXattr() == null ? "{}".getBytes() : existing.getXattr());
            this.command = (BinarySubdocMultiCommand) cmd;
            this.client = client;
            this.specs = command.getLookupSpecs();
            this.cache = cache;
            this.needCreate = needCreate;
            this.hasXattrSpec = false;
            results = new ArrayList<SpecResult>();
        }

        boolean isMutator() {
            return command.getComCode() == CommandCode.SUBDOC_MULTI_MUTATION;
        }

        private BinaryResponse handleLookupSpec(BinarySubdocMultiCommand.MultiSpec spec, int index) {
            Operation op = spec.getOp();
            if (op == null) {
                results.add(new SpecResult(index, ErrorCode.UNKNOWN_COMMAND));
                return null;
            }
            if (!op.isLookup()) {
                return new BinaryResponse(command, ErrorCode.SUBDOC_INVALID_COMBO);
            }
            boolean isXattr = (spec.getFlags() & BinarySubdocCommand.PATHFLAG_XATTR) != 0;
            ResultInfo rsi = SubdocCommandExecutor.executeSubdocLookup(op, isXattr ? currentAttrs : currentDoc, spec.getPath());
            switch (rsi.getStatus()) {
                case SUCCESS:
                    if (op.returnsMatch()) {
                        results.add(new SpecResult(index, rsi.getMatchString()));
                    } else {
                        results.add(new SpecResult(index, ErrorCode.SUCCESS));
                    }
                    return null;
                case SUBDOC_DOC_NOTJSON:
                case SUBDOC_DOC_E2DEEP:
                    return new BinaryResponse(command, rsi.getStatus());
                default:
                    results.add(new SpecResult(index, rsi.getStatus()));
                    return null;
            }
        }

        private BinaryResponse buildMutationError(ErrorCode ec, int index) {
            ByteBuffer bb = ByteBuffer.allocate(3);
            bb.put((byte) index);
            bb.putShort(ec.value());
            ErrorCode topLevelRc;
            if (ec == ErrorCode.SUBDOC_INVALID_COMBO) {
                topLevelRc = ec;
            } else {
                topLevelRc = ErrorCode.SUBDOC_MULTI_FAILURE;
            }
            return BinaryResponse.createWithValue(topLevelRc, command, Datatype.RAW.value(), bb.array(), 0);
        }

        private ResultInfo handleMutationSpecInner(Operation op, String input,
                                                   BinarySubdocMultiMutationCommand.MultiSpec spec)
                throws MutationError {
            byte specFlags = spec.getFlags();
            if ((command.getSubdocDocFlags() & BinarySubdocCommand.DOCFLAG_CREATEMASK) != 0) {
                specFlags |= BinarySubdocCommand.PATHFLAG_MKDIR_P;
            }
            ResultInfo rsi = SubdocCommandExecutor.executeSubdocOperation(op, input, spec.getPath(),
                    spec.getValue(), specFlags);
            if (rsi.getStatus() != ErrorCode.SUCCESS) {
                throw new MutationError(rsi.getStatus());
            }
            return rsi;
        }

        private BinaryResponse handleMutationSpec(BinarySubdocMultiCommand.MultiSpec spec, int index) {
            Operation op = spec.getOp();

            if (op == null) {
                return buildMutationError(ErrorCode.UNKNOWN_COMMAND, index);
            }

            if (!op.isMutator()) {
                return buildMutationError(ErrorCode.SUBDOC_INVALID_COMBO, index);
            }

            boolean isXattr = (spec.getFlags() & BinarySubdocCommand.PATHFLAG_XATTR) != 0;
            ResultInfo rsi;
            try {
                if (isXattr) {
                    rsi = handleMutationSpecInner(op, currentAttrs, spec);
                    currentAttrs = rsi.getNewDocString();
                    hasXattrSpec = true;
                } else {
                    rsi = handleMutationSpecInner(op, currentDoc, spec);
                    currentDoc = rsi.getNewDocString();
                }
            } catch (MutationError ex) {
                return buildMutationError(ex.code, index);
            }

            if (op.returnsMatch()) {
                results.add(new SpecResult(index, rsi.getMatchString()));
            }
            return null;
        }

        BinaryResponse execute() {
            for (int i = 0; i < specs.size(); i++) {
                BinarySubdocMultiCommand.MultiSpec spec = specs.get(i);
                BinaryResponse result;
                if (isMutator()) {
                    result = handleMutationSpec(spec, i);
                } else {
                    result = handleLookupSpec(spec, i);
                }
                if (result != null) {
                    return result;
                }
            }

            if (isMutator()) {
                MutationInfoWriter miw = client.getMutinfoWriter();
                byte[] newXattrs;
                if (hasXattrSpec) {
                    newXattrs = currentAttrs.getBytes();
                } else if (needCreate) {
                    newXattrs = null;
                } else {
                    newXattrs = existing.getXattr();
                }
                Item newItem = new Item(
                        existing.getKeySpec(),
                        existing.getFlags(),
                        command.getNewExpiry(existing.getExpiryTime()),
                        currentDoc.getBytes(),
                        newXattrs,
                        command.getCas(), Datatype.RAW.value());

                MutationStatus ms;
                if (needCreate) {
                    needCreate = false;
                    ms = cache.add(newItem, client.supportsXerror());
                    if (ms.getStatus() == ErrorCode.KEY_EEXISTS) {
                        results.clear();
                        return execute();
                    }
                } else {
                    ms = cache.replace(newItem, client.supportsXerror());
                }

                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                for (SpecResult result : results) {
                    int specLen = 3;

                    if (result.ec == ErrorCode.SUCCESS) {
                        specLen += 4;
                        specLen += result.value.length();
                    }

                    ByteBuffer bb = ByteBuffer.allocate(specLen);
                    bb.put((byte) result.index);
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
                return new BinaryResponse(command, ms, miw, Datatype.RAW.value(), newItem.getCas(), bao.toByteArray());
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
                byte[] multiPayload = bao.toByteArray();
                ErrorCode finalEc = hasError ? ErrorCode.SUBDOC_MULTI_FAILURE : ErrorCode.SUCCESS;
                return BinaryResponse.createWithValue(finalEc, command, Datatype.RAW.value(), multiPayload, existing.getCas());
            }
        }

        private class MutationError extends Exception {
            ErrorCode code;

            MutationError(ErrorCode ec) {
                code = ec;
            }
        }
    }
}
