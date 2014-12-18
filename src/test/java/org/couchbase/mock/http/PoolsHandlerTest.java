/*
 * Copyright 2011 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.http;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import junit.framework.TestCase;
import org.couchbase.mock.CouchbaseMock;

/**
 * Verify that our "/pools" JSON is "correct"
 *
 * @author Trond Norbye
 */
public class PoolsHandlerTest extends TestCase {

    private CouchbaseMock mock;

    public PoolsHandlerTest(String testName) throws IOException {
        super(testName);
        mock = new CouchbaseMock("localhost", 0, 1, 1 );
    }

    /**
     * Test of getPoolsJSON method, of class PoolsHandler.
     */
    public void testGetPoolsJSON() {
        PoolsHandler instance = new PoolsHandler(mock);
        assertNotNull(instance);
        String result = StateGrabber.getAllPoolsJSON(mock);
        JsonObject jObj = new Gson().fromJson(result, JsonObject.class);
        JsonArray poolsArray;
        assertNotNull(jObj);
        assertTrue(jObj.has("isAdminCreds"));
        assertTrue(jObj.get("isAdminCreds").getAsBoolean());
        assertTrue(jObj.has("pools"));

        poolsArray = jObj.getAsJsonArray("pools");
        assertNotNull(jObj);

        JsonObject firstPool = poolsArray.get(0).getAsJsonObject();
        assertNotNull(firstPool);
        assertEquals(firstPool.get("name").getAsString(), "default");
        assertEquals(firstPool.get("streamingUri").getAsString(),
                "/poolsStreaming/default");
        assertEquals(firstPool.get("uri").getAsString(),
                "/pools/default");

    }
}
