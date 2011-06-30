/**
 *     Copyright 2011 Membase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.couchbase.mock.memcached;

import java.nio.ByteBuffer;

/**
 * @author Trond Norbye
 *
 */
public class BinaryResponse {

    private ByteBuffer buffer;
    private byte bytes[];
    private ErrorCode errorCode;
    private ComCode comCode;

    public BinaryResponse(BinaryCommand command, ErrorCode errorCode) {
        create(command, errorCode, 0);
    }

    public BinaryResponse(BinaryCommand command, ErrorCode errorCode, long cas) {
        create(command, errorCode, cas);
    }

    public BinaryResponse(BinaryCommand command, Item item) {
        create(command, item);
    }

    private void create(BinaryCommand command, ErrorCode errorCode, long cas) {
        this.errorCode = errorCode;
        comCode = command.getComCode();

        buffer = ByteBuffer.allocate(24);
        buffer.put((byte) 0x81);
        buffer.put(command.getComCode().cc());
        buffer.putShort(6, errorCode.value());
        buffer.putInt(12, command.getOpaque());
        buffer.putLong(16, cas);
        buffer.rewind();
    }

    private void create(BinaryCommand command, Item item) {
        this.errorCode = ErrorCode.SUCCESS;
        comCode = command.getComCode();

        short keylen = 0;
        if (command.getComCode() == ComCode.GETK || command.getComCode() == ComCode.GETKQ) {
            keylen = (short) item.getKey().length();
        }

        buffer = ByteBuffer.allocate(28 + keylen + item.getValue().length);

        buffer.put((byte) 0x81);
        buffer.put(command.getComCode().cc());
        buffer.putShort(keylen);
        buffer.put((byte) 4); // (the flags)
        buffer.putShort(6, errorCode.value());
        buffer.putInt(8, keylen + 4 + item.getValue().length);
        buffer.putInt(12, command.getOpaque());
        buffer.putLong(16, item.getCas());
        buffer.putInt(24, item.getFlags());
        buffer.position(28);
        if (keylen > 0) {
            buffer.put(item.getKey().getBytes());
        }
        buffer.put(item.getValue());
        buffer.rewind();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public byte[] getData() {
        return bytes;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public ComCode getComCode() {
        return comCode;
    }
}
