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
import com.couchbase.mock.memcached.protocol.BinarySubdocCommand;
import com.couchbase.mock.memcached.protocol.ErrorCode;
import com.couchbase.mock.subdoc.*;

public class SubdocCommandExecutor implements CommandExecutor {

    public static class ResultInfo {
        private final Result result;
        private final ErrorCode status;
        ResultInfo(Result result, ErrorCode status) {
            this.result = result;
            this.status = status;
        }
        public String getNewDocString() {
            return result.getNewDocString();
        }
        public String getMatchString() {
            return result.getMatchString();
        }
        public ErrorCode getStatus() {
            return status;
        }
    }

    static ResultInfo executeSubdocOperation(Operation op, String doc, String path, String value, byte flags) {
        ErrorCode ec = ErrorCode.SUCCESS;
        Result result = null;
        boolean isMkdirP = (flags & (BinarySubdocCommand.PATHFLAG_MKDIR_P | BinarySubdocCommand.DOCFLAG_MKDOC)) != 0;
        try {
            result = Executor.execute(doc, path, op, value, isMkdirP);
        } catch (PathNotFoundException ex2) {
            ec = ErrorCode.SUBDOC_PATH_ENOENT;
        } catch (PathExistsException ex3) {
            ec = ErrorCode.SUBDOC_PATH_EEXISTS;
        } catch (BadNumberException ex4_1) {
            ec = ErrorCode.SUBDOC_DELTA_ERANGE;
        } catch (EmptyValueException ex4) {
            ec = ErrorCode.SUBDOC_VALUE_CANTINSERT;
        } catch (DocNotJsonException ex5) {
            ec = ErrorCode.SUBDOC_DOC_NOTJSON;
        } catch (InvalidPathException ex6) {
            ec = ErrorCode.SUBDOC_PATH_EINVAL;
        } catch (NumberTooBigException ex7) {
            ec = ErrorCode.SUBDOC_NUM_ERANGE;
        } catch (DeltaTooBigException ex8) {
            ec = ErrorCode.SUBDOC_DELTA_ERANGE;
        } catch (CannotInsertException ex9) {
            ec = ErrorCode.SUBDOC_VALUE_CANTINSERT;
        } catch (PathParseException ex10) {
            ec = ErrorCode.SUBDOC_PATH_EINVAL;
        } catch (PathMismatchException ex11) {
            ec = ErrorCode.SUBDOC_PATH_MISMATCH;
        } catch (ZeroDeltaException ex12) {
            ec = ErrorCode.SUBDOC_DELTA_ERANGE;
        } catch (SubdocException exFallback) {
            throw new RuntimeException(exFallback);
        }
        return new ResultInfo(result, ec);
    }

    public static ResultInfo executeSubdocLookup(Operation op, String doc, String path) {
        return executeSubdocOperation(op, doc, path, null, (byte)0);
    }

    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        BinarySubdocCommand command = (BinarySubdocCommand)cmd;
        Operation subdocOp = command.getSubdocOp();
        VBucketStore cache = server.getCache(cmd);
        SubdocItem subdocInput = command.getItem();

        boolean isMkdoc = (command.getSubdocDocFlags() & BinarySubdocCommand.DOCFLAG_CREATEMASK) != 0;
        boolean isXattr = (command.getSubdocPathFlags() & BinarySubdocCommand.PATHFLAG_XATTR) != 0;
        boolean needsCreate = false;

        if (isMkdoc && !subdocOp.isCreative()) {
            client.sendResponse(new BinaryResponse(cmd, ErrorCode.EINVAL));
            return;
        }

        Item existing = cache.get(subdocInput.getKeySpec());

        if (existing == null) {
            if (!isMkdoc) {
                client.sendResponse(new BinaryResponse(cmd, ErrorCode.KEY_ENOENT));
                return;
            }

            String newValue = Executor.getRootType(subdocInput.getPath(), subdocOp);
            if (newValue == null) {
                client.sendResponse(new BinaryResponse(cmd, ErrorCode.KEY_ENOENT));
                return;
            }
            byte[] newBody, newAttr;
            if (isXattr) {
                newBody = null;
                newAttr = newValue.getBytes();
            } else {
                newAttr = null;
                newBody = newValue.getBytes();
            }
            existing = new Item(subdocInput.getKeySpec(), 0, 0, newBody, newAttr, 0);
            needsCreate = true;

        } else {
            if ((command.getSubdocDocFlags() & BinarySubdocCommand.DOCFLAG_ADD) != 0) {
                client.sendResponse(new BinaryResponse(command, ErrorCode.KEY_EEXISTS));
                return;
            }
        }

        if (command.getCas() != 0) {
            subdocInput.setCas(command.getCas());
        } else {
            subdocInput.setCas(existing.getCas());
        }

        byte[] curValue;
        if (isXattr) {
            curValue = existing.getXattr();
            if (curValue == null) {
                curValue = "{}".getBytes();
            }
        } else {
            curValue = existing.getValue();
            if (curValue == null) {
                curValue = "".getBytes();
            }
        }

        ResultInfo rci = executeSubdocOperation(subdocOp,
                new String(curValue),
                subdocInput.getPath(),
                new String(subdocInput.getValue()),
                command.getSubdocPathFlags());

        if (rci.getStatus() != ErrorCode.SUCCESS) {
            client.sendResponse(new BinaryResponse(cmd, rci.getStatus()));
            return;
        }

        byte[] value = null;
        if (subdocOp.returnsMatch()) {
            value = rci.getMatchString().getBytes();
        }

        if (subdocOp.isMutator()) {
            MutationStatus ms;
            MutationInfoWriter miw = client.getMutinfoWriter();
            byte[] xattr, body;
            if (isXattr) {
                xattr = rci.getNewDocString().getBytes();
                body = existing.getValue();
            } else {
                xattr = existing.getXattr();
                body = rci.getNewDocString().getBytes();
            }
            Item newItm = new Item(
                    existing.getKeySpec(), existing.getFlags(), subdocInput.getExpiryTime(),
                    body, xattr, subdocInput.getCas());
            if (needsCreate) {
                ms = cache.add(newItm);
                if (ms.getStatus() == ErrorCode.KEY_EEXISTS) {
                    execute(cmd, server, client);
                    return;
                }
            } else {
                ms = cache.replace(newItm);
            }

            if (ms.getStatus() == ErrorCode.SUCCESS) {
                client.sendResponse(new BinaryResponse(cmd, ms, miw, newItm.getCas(), value));
            } else {
                client.sendResponse(new BinaryResponse(cmd, ms.getStatus()));
            }
        } else {
            client.sendResponse(BinaryResponse.createWithValue(command, value, existing.getCas()));
        }
    }
}
