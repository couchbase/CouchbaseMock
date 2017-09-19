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

package com.couchbase.mock.http.query;

import java.util.Map;

public class ErrorState {
    private final String message;
    private final int code;
    private final boolean failRegular;
    private final boolean failPrepared;

    public ErrorState(String message, int code, boolean failRegular, boolean failPrepared) {
        this.message = message;
        this.code = code;
        this.failRegular = failRegular;
        this.failPrepared = failPrepared;
    }

    public String getMessage() {
        return message;
    }

    public int getCode() {
        return code;
    }

    public boolean shouldReturnError(Map<String, Object> mm) {
        if (mm.containsKey("statement")) {
            return failRegular;
        } else {
            return failPrepared;
        }
    }
}
