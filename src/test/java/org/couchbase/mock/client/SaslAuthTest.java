package org.couchbase.mock.client;

import org.couchbase.mock.memcached.client.ClientResponse;
import org.couchbase.mock.memcached.client.CommandBuilder;
import org.couchbase.mock.memcached.client.MemcachedClient;
import org.couchbase.mock.memcached.protocol.CommandCode;
import org.couchbase.mock.memcached.protocol.ErrorCode;

import java.nio.ByteBuffer;

/**
 * Test that various binary level functionality works as expected
 * @author Mark Nunberg
 */
public class SaslAuthTest extends ClientBaseTest {
    @Override
    protected void setUp() throws Exception {
        createMock("protected", "secret");
    }

    private byte[] buildPLAIN(String id, String user, String pass) {
        ByteBuffer buf = ByteBuffer.allocate(id.length()+user.length()+pass.length() + 2);
        buf.put(id.getBytes());
        buf.put((byte)0x00);
        buf.put(user.getBytes());
        buf.put((byte)0x00);
        buf.put(pass.getBytes());
        return buf.array();
    }

    public void testSaslAuth() throws Exception {
        MemcachedClient binClient = getBinClient(0);
        CommandBuilder cBuilder = new CommandBuilder(CommandCode.SASL_AUTH);
        ClientResponse resp;

        cBuilder.value(buildPLAIN("foo", "protected", "secret"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.SUCCESS, resp.getStatus());

        // Try with a bad password
        cBuilder.value("blah");
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        // Missing ID; still have a NUL
        cBuilder.value(buildPLAIN("", "protected", "secret"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.SUCCESS, resp.getStatus());

        // Bad password
        cBuilder.value(buildPLAIN("id", "protected", "bad"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        // Empty
        cBuilder.value(buildPLAIN("", "", ""));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        // Empty password
        cBuilder.value(buildPLAIN("foo", "protected", ""));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        cBuilder.value(buildPLAIN("foo", "", "secret"));
        resp = binClient.sendRequest(cBuilder);
        assertEquals(ErrorCode.AUTH_ERROR, resp.getStatus());

        binClient.close();

    }
}
