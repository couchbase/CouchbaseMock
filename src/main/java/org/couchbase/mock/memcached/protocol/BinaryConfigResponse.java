package org.couchbase.mock.memcached.protocol;

import org.couchbase.mock.memcached.MemcachedServer;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mark Nunberg
 */
public class BinaryConfigResponse extends BinaryResponse {

    @SuppressWarnings("unused")
    private BinaryConfigResponse(ByteBuffer buf) {
        super(buf);
        throw new UnsupportedOperationException();
    }

    /**
     * Create a new response which contains a cluster configuration, if supported
     * @param command The command received
     * @param server The server on which the command was received
     * @param errOk Error code to be supplied if the configuration can be sent
     * @param errNotSupp Error code to be supplied if the configuration cannot
     * be sent
     * @return A response object
     */
    public static BinaryResponse create(BinaryCommand command, MemcachedServer server, ErrorCode errOk, ErrorCode errNotSupp) {
        if (!server.isCccpEnabled()) {
            return new BinaryResponse(command, errNotSupp);
        }

        String config = server.getBucket().getJSON();
        config = config.replaceAll(Pattern.quote(server.getHostname()),
                                   Matcher.quoteReplacement("$HOST"));

        byte[] jsBytes = config.getBytes();
        ByteBuffer buf = create(command, errOk, 0, 0, jsBytes.length, 0);
        buf.put(jsBytes);
        buf.rewind();
        return new BinaryResponse(buf);
    }

    /**
     * Creates a response for {@code CMD_GET_CONFIG}
     * @param command The command received
     * @param server The server
     * @return A response containing the config with a successful code, or
     * an empty response with NOT_SUPPORTED
     */
    public static BinaryResponse createGetConfig(BinaryCommand command, MemcachedServer server) {
        return create(command, server, ErrorCode.SUCCESS, ErrorCode.NOT_SUPPORTED);
    }

    /**
     * Creates a response for {@code NOT_MY_VBUCKET} conditions
     * @param command The command which attempted to access the wrong vbucket
     * @param server The bucket which we own
     * @return A command of status {@code NOT_MY_VBUCKET}. This will contain
     * a config payload, if supported
     */
    public static BinaryResponse createNotMyVbucket(BinaryCommand command, MemcachedServer server) {
        return create(command, server, ErrorCode.NOT_MY_VBUCKET, ErrorCode.NOT_MY_VBUCKET);
    }
}
