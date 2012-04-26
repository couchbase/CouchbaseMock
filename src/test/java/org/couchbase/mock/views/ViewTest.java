/**
 *     Copyright 2012 Couchbase, Inc.
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
package org.couchbase.mock.views;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import junit.framework.TestCase;
import net.sf.json.JSON;
import net.sf.json.JSONObject;
import org.couchbase.mock.CouchbaseBucket;
import org.couchbase.mock.memcached.DataStore;
import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.MemcachedServer;

import net.sf.json.JSONSerializer;

/**
 *
 * @author Sergey Avseyev
 */
public class ViewTest extends TestCase {

    public void testParser() {
        String body = "{"
                + "  \"_id\": \"_design/blog\","
                + "  \"language\": \"javascript\","
                + "  \"views\": {"
                + "    \"recent_posts\": {"
                + "      \"map\": \"function(doc){ if(doc.date && doc.title){emit(doc.date, doc.title);} }\""
                + "    }"
                + "  }"
                + "}";
        DesignDocument ddoc = new DesignDocument(body);
        assertEquals("_design/blog", ddoc.getId());
        assertEquals(body, ddoc.getBody());
        ArrayList<View> views = ddoc.getViews();
        assertNotNull(views);
        assertEquals(1, views.size());
        View recentPosts = views.get(0);
        assertEquals("recent_posts", recentPosts.getName());
        assertEquals("function(doc){ if(doc.date && doc.title){emit(doc.date, doc.title);} }", recentPosts.getMapSource());
        assertEquals(null, recentPosts.getReduceSource());
    }

    public void testDecodingPrimitives() throws ScriptException {
        ScriptEngine jsEngine = new ScriptEngineManager().getEngineByName("javascript");
        String wrapper = "(function(){ return XXX; })()";

        assertEquals(Long.class, View.fromNativeObject(jsEngine.eval(wrapper.replace("XXX", "123"))).getClass());
        assertEquals(Double.class, View.fromNativeObject(jsEngine.eval(wrapper.replace("XXX", "123.567"))).getClass());
        assertEquals(ArrayList.class, View.fromNativeObject(jsEngine.eval(wrapper.replace("XXX", "[123, 567]"))).getClass());
        assertEquals(HashMap.class, View.fromNativeObject(jsEngine.eval(wrapper.replace("XXX", "{'123': 567}"))).getClass());
    }

    public void testDecodingCompound() throws ScriptException {
        ScriptEngine jsEngine = new ScriptEngineManager().getEngineByName("javascript");
        String wrapper = "(function(){ return XXX; })()";
        String compound = "[{'foo': [1.3, 'hello']}, {'bar': {'who': 'world'}}]";
        Object value = View.fromNativeObject(jsEngine.eval(wrapper.replace("XXX", compound)));

        assertEquals(ArrayList.class, value.getClass());
        ArrayList array = (ArrayList) value;
        assertEquals(2, array.size());

        assertEquals(HashMap.class, array.get(0).getClass());
        HashMap first = (HashMap) array.get(0);
        assertEquals(1, first.size());
        assertTrue(first.containsKey("foo"));
        assertEquals(ArrayList.class, first.get("foo").getClass());
        ArrayList foo = (ArrayList) first.get("foo");
        assertEquals(1.3, foo.get(0));
        assertEquals("hello", foo.get(1));

        assertEquals(HashMap.class, array.get(1).getClass());
        HashMap second = (HashMap) array.get(1);
        assertEquals(1, second.size());
        assertTrue(second.containsKey("bar"));
        assertEquals(HashMap.class, second.get("bar").getClass());
        HashMap bar = (HashMap) second.get("bar");
        assertTrue(bar.containsKey("who"));
        assertEquals("world", bar.get("who"));
    }

    private DataStore seedDocuments(int num) throws IOException {
        /* single server and 16 vbuckets */
        CouchbaseBucket bucket = new CouchbaseBucket("test", "127.0.0.1", 9000, 1, 0, 16);
        MemcachedServer server = bucket.getServers()[0];
        DataStore store = bucket.getDatastore();
        for (int i = 0; i < num; i++) {
            String key = String.format("key-%03d", i);
            byte[] val = ( "{\"val\": " + Integer.toString(i) + "}" ).getBytes();
            store.add(server, (short) ( i % 16 ), new Item(key, 12345678, 0, val, 0));
        }
        return store;
    }

    public void testMapperTrivial() throws ScriptException, IOException {
        DataStore store = seedDocuments(40);
        View view = new View("all", "function(doc){emit(doc._id, null)}", null);
        HashMap results = view.execute(store); /* execute with default config */

        Integer total_rows = (Integer) results.get("total_rows");
        assertEquals(40, (int) total_rows);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(40, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("key-000", (String) firstRow.get("key"));
    }

    public void testSkipLimit() throws ScriptException, IOException {
        DataStore store = seedDocuments(40);
        View view = new View("all", "function(doc){emit(doc._id, null)}", null);
        Configuration config = new Configuration();
        config.setSkip(10);
        config.setLimit(5);
        HashMap results = view.execute(store, config);

        Integer total_rows = (Integer) results.get("total_rows");
        assertEquals(40, (int) total_rows);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(5, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("key-010", (String) firstRow.get("key"));
    }

    public void testRanging() throws ScriptException, IOException {
        DataStore store = seedDocuments(40);
        View view = new View("all", "function(doc){emit(doc.val); emit(doc.val+1);}", null);

        Configuration config = new Configuration();
        config.setStartKey("1");
        config.setEndKey("3");
        /* filter results: 1, 1, 2, 2, 3, 3 */
        HashMap results = view.execute(store, config);
        Integer total_rows = (Integer) results.get("total_rows");
        assertEquals(80, (int) total_rows);
        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(6, rows.size());
        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("1", firstRow.get("key").toString());
        assertEquals("key-000", (String) firstRow.get("id"));

        /* filter results: 3, 3, 2, 2, 1, 1 */
        config.setDescending(true);
        results = view.execute(store, config);
        total_rows = (Integer) results.get("total_rows");
        assertEquals(80, (int) total_rows);
        rows = (ArrayList) results.get("rows");
        assertEquals(6, rows.size());
        firstRow = (HashMap) rows.get(0);
        assertEquals("3", firstRow.get("key").toString());
        assertEquals("key-003", (String) firstRow.get("id"));
    }

    public void testRangingExclusiveEnd() throws ScriptException, IOException {
        DataStore store = seedDocuments(40);
        View view = new View("all", "function(doc){emit(doc.val); emit(doc.val+1);}", null);

        Configuration config = new Configuration();
        config.setStartKey("1");
        config.setEndKey("3");
        config.setInclusiveEnd(false);
        /* filter results: 1, 1, 2, 2 */
        HashMap results = view.execute(store, config);
        Integer total_rows = (Integer) results.get("total_rows");
        assertEquals(80, (int) total_rows);
        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(4, rows.size());
    }

    public void testMapperDescending() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all", "function(doc){emit(doc._id, null)}", null);
        Configuration config = new Configuration();
        config.setDescending(true);
        HashMap results = view.execute(store, config);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(9, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("key-008", (String) firstRow.get("key"));
    }

    public void testMapperIncludeDocs() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all", "function(doc){emit(doc._id, null)}", null);
        Configuration config = new Configuration();
        config.setIncludeDocs(true);
        HashMap results = view.execute(store, config);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(9, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("key-000", (String) firstRow.get("key"));
        assertEquals("{\"val\":0,\"$flags\":12345678,\"$exp\":0}", firstRow.get("doc").toString());
    }

    public void testMapperEmittingCustomKey() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all", "function(doc){emit(doc._id.toUpperCase(), (doc.val + 1).toString())}", null);
        HashMap results = view.execute(store);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(9, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("key-000", (String) firstRow.get("id"));
        assertEquals("KEY-000", (String) firstRow.get("key"));
        assertEquals("1", (String) firstRow.get("value"));
        assertEquals(null, firstRow.get("doc"));
    }

    public void testReduceCount() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all",
                "function(doc){emit(doc._id)}",
                "function(keys, values, rereduce){ return values.length; }");
        HashMap results = view.execute(store);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        assertEquals(9, ( (Number) firstRow.get("value") ).intValue());
    }

    public void testReduceCountBuiltin() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all", "function(doc){emit(doc._id)}", "_count");
        HashMap results = view.execute(store);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        assertEquals(9, ( (Number) firstRow.get("value") ).intValue());
    }

    public void testReduceSum() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all",
                "function(doc){emit(doc._id, doc.val)}",
                "function(keys, values, rereduce){ return sum(values); }");
        HashMap results = view.execute(store);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        assertEquals(36, ( (Number) firstRow.get("value") ).intValue());
    }

    public void testReduceSumBuiltin() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all", "function(doc){emit(doc._id, doc.val)}", "_sum");
        HashMap results = view.execute(store);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        assertEquals(36, ( (Number) firstRow.get("value") ).intValue());
    }

    public void testReduceStatsBuiltin() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all", "function(doc){emit(doc._id, doc.val)}", "_stats");
        HashMap results = view.execute(store);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        HashMap stats = (HashMap) firstRow.get("value");
        assertEquals(36, ( (Number) stats.get("sum") ).intValue());
        assertEquals(204, ( (Number) stats.get("sumsqr") ).intValue());
        assertEquals(9, ( (Number) stats.get("count") ).intValue());
        assertEquals(0, ( (Number) stats.get("min") ).intValue());
        assertEquals(8, ( (Number) stats.get("max") ).intValue());
    }

    public void testJsonOrdering() {
        JSON a, b;

        a = JSONSerializer.toJSON("[1, '10']");
        b = JSONSerializer.toJSON("[1, '9']");
        assertTrue(View.RowComparator.jsonCompareTo(a, b) < 0);

        a = JSONSerializer.toJSON("[1, 10]");
        b = JSONSerializer.toJSON("[1, 9]");
        assertTrue(View.RowComparator.jsonCompareTo(a, b) > 0);

        a = JSONSerializer.toJSON("{'foo': 1, 'bar': 2}");
        b = JSONSerializer.toJSON("{'bar': 2, 'foo': 1}");
        assertTrue(View.RowComparator.jsonCompareTo(a, b) == 0);

        a = JSONSerializer.toJSON("[1, {'foo': 1, 'bar': 2}]");
        b = JSONSerializer.toJSON("[1, {'bar': 2, 'foo': 1}]");
        assertTrue(View.RowComparator.jsonCompareTo(a, b) == 0);
    }

    public void testReduceGroupCount() throws IOException, ScriptException {
        DataStore store = seedDocuments(9);
        View view = new View("all",
                "function(doc){if (doc.val % 2 == 0) { emit([\"odd\", doc._id])} else { emit([\"even\", doc._id]) } }",
                "function(keys, values, rereduce){ return values.length; }");

        HashMap results = view.execute(store);
        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());
        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        assertEquals(9, ( (Number) firstRow.get("value") ).intValue());

        Configuration config = new Configuration();
        config.setGroup(true);
        results = view.execute(store, config);
        rows = (ArrayList) results.get("rows");
        assertEquals(9, rows.size());
        firstRow = (HashMap) rows.get(0);
        assertEquals("[\"even\",\"key-001\"]", firstRow.get("key").toString());
        assertEquals(1, ( (Number) firstRow.get("value") ).intValue());

        config = new Configuration();
        config.setGroup(true);
        config.setGroupLevel(1);
        results = view.execute(store, config);
        rows = (ArrayList) results.get("rows");
        assertEquals(2, rows.size());
        firstRow = (HashMap) rows.get(0);
        assertEquals("[\"even\"]", firstRow.get("key").toString());
        assertEquals(4, ( (Number) firstRow.get("value") ).intValue());

        config = new Configuration();
        config.setGroup(true);
        config.setGroupLevel(0);
        results = view.execute(store);
        rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());
        firstRow = (HashMap) rows.get(0);
        assertEquals("null", firstRow.get("key").toString());
        assertEquals(9, ( (Number) firstRow.get("value") ).intValue());

        config = new Configuration();
        config.setGroup(true);
        config.setGroupLevel(10);
        results = view.execute(store, config);
        rows = (ArrayList) results.get("rows");
        assertEquals(9, rows.size());
        firstRow = (HashMap) rows.get(0);
        assertEquals("[\"even\",\"key-001\"]", firstRow.get("key").toString());
        assertEquals(1, ( (Number) firstRow.get("value") ).intValue());
    }

    public void testItAllowsToTurnOffReduce() throws ScriptException, IOException {
        DataStore store = seedDocuments(40);
        View view = new View("all", "function(doc){emit(doc._id, null)}", "_count");
        Configuration config = new Configuration();
        config.setReduce(false);
        HashMap results = view.execute(store, config);

        Integer total_rows = (Integer) results.get("total_rows");
        assertEquals(40, (int) total_rows);

        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(40, rows.size());

        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("key-000", (String) firstRow.get("key"));
    }

    public void testFilters() throws ScriptException, IOException {
        DataStore store = seedDocuments(40);
        View view = new View("all", "function(doc){emit({'id': doc._id}, null)}");

        Configuration config = new Configuration();
        config.setKey("{    'id'   :   'key-001'    }");
        HashMap results = view.execute(store, config);
        Integer total_rows = (Integer) results.get("total_rows");
        assertEquals(40, (int) total_rows);
        ArrayList rows = (ArrayList) results.get("rows");
        assertEquals(1, rows.size());
        HashMap firstRow = (HashMap) rows.get(0);
        assertEquals("{\"id\":\"key-001\"}", JSONObject.fromObject(firstRow.get("key")).toString());

        config = new Configuration();
        ArrayList keys = new ArrayList();
        keys.add("{'id': 'key-006'}");
        keys.add("{'id': 'key-008'}");
        config.setKeys(keys);
        results = view.execute(store, config);
        total_rows = (Integer) results.get("total_rows");
        assertEquals(40, (int) total_rows);
        rows = (ArrayList) results.get("rows");
        assertEquals(2, rows.size());
        firstRow = (HashMap) rows.get(0);
        assertEquals("{\"id\":\"key-006\"}", JSONObject.fromObject(firstRow.get("key")).toString());
    }

}
