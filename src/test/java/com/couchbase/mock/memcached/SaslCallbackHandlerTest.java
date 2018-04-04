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
 * @author sozer
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
