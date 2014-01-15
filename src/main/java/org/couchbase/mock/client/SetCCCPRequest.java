package org.couchbase.mock.client;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import java.util.Collection;

/**
 * @author Mark Nunberg
 */
public class SetCCCPRequest extends MockRequest {
    public SetCCCPRequest(boolean mode) {
        super();
        setName("set_cccp");
        payload.put("enabled", mode);
    }

    public SetCCCPRequest(boolean mode, String bucket, Collection<Integer> servers) {
        this(mode);

        payload.put("bucket", bucket);
        JsonArray arr = new JsonArray();

        for (int ix : servers) {
            arr.add(new JsonPrimitive(ix));
        }

        payload.put("servers", arr);
    }
}