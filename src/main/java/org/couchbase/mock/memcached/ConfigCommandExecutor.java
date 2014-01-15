package org.couchbase.mock.memcached;

import org.couchbase.mock.memcached.protocol.BinaryCommand;
import org.couchbase.mock.memcached.protocol.BinaryConfigResponse;

/**
 * Created by mnunberg on 1/15/14.
 */
public class ConfigCommandExecutor implements CommandExecutor {
    @Override
    public void execute(BinaryCommand cmd, MemcachedServer server, MemcachedConnection client) {
        client.sendResponse(BinaryConfigResponse.createGetConfig(cmd, server));
    }
}
