/*
 * Copyright 2018 Couchbase, Inc.
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

import java.io.IOException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Senol Ozer / Amadeus IT Group
 */
public class SaslCallbackHandlerTest {

    private SaslCallbackHandler callbackHandler;

    @Before
    public void setUp() {
        callbackHandler = new SaslCallbackHandler("foo", "bar");
    }

    @Test
    public void testNameCallback() throws IOException, UnsupportedCallbackException {
        NameCallback nameCallcak = new NameCallback("foo");
        callbackHandler.handle(new Callback[] {nameCallcak});
        Assert.assertEquals("foo", nameCallcak.getName());
    }

    @Test
    public void testPasswordCallback() throws IOException, UnsupportedCallbackException {
        PasswordCallback passwordCallcak = new PasswordCallback("test", true);
        callbackHandler.handle(new Callback[] {passwordCallcak});
        Assert.assertEquals("bar", new String(passwordCallcak.getPassword()));
    }



}
