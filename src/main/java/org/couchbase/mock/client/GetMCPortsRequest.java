package org.couchbase.mock.client;

import org.jetbrains.annotations.Nullable;

/**
 * @author Mark Nunberg
 */
public class GetMCPortsRequest extends MockRequest {
    public GetMCPortsRequest(@Nullable String bucket) {
        super();
        setName("get_mcports");
        if (bucket != null) {
            payload.put("bucket", bucket);
        }
    }

    public GetMCPortsRequest() {
        this(null);
    }
}
