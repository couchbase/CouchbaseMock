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

import net.spy.memcached.internal.OperationFuture;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.client.ClientBaseTest;
import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.Storage;

import java.io.IOException;
import java.rmi.ConnectIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Sergey Avseyev
 */
public class ViewTest extends ClientBaseTest {

    public void testParser() throws Exception {
        String body = "{"
                + "  \"_id\": \"_design/blog\","
                + "  \"language\": \"javascript\","
                + "  \"views\": {"
                + "    \"recent_posts\": {"
                + "      \"map\": \"function(doc){ if(doc.date && doc.title){emit(doc.date, doc.title);} }\""
                + "    }"
                + "  }"
                + "}";
        DesignDocument ddoc = DesignDocument.create(body, "blog");
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

    private Iterable<Item> seedDocuments(int num) throws IOException {
        ArrayList<OperationFuture> ops = new ArrayList<OperationFuture>();
        for (int i = 0; i < num; i++) {
            String key = String.format("key-%03d", i);
            String val = String.format("{\"val\":%d}", i);
            ops.add(client.set(key, val));
        }
        for (OperationFuture op : ops) {
            try {
                op.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (!op.getStatus().isSuccess()) {
                throw new RuntimeException("Operation failed to execute!");
            }
            String ss = (String) client.get(op.getKey());
            assertNotNull(ss);
            assertNotSame("", ss);
        }

        // Now, get the vBucket store.. :(
        Bucket bucket = couchbaseMock.getBuckets().get("default");
        return bucket.getMasterItems(Storage.StorageType.CACHE);
    }

    public void testMapperTrivial() throws Exception {
        Iterable<Item> store = seedDocuments(40);
        View view = new View("all", "function(doc,meta){emit(meta.id, null)}", null);
        QueryResult results = view.execute(store); /* execute with default config */

        assertEquals(40, results.getTotalRowCount());
        assertEquals(40, results.getFilteredRowCount());
        assertEquals("key-000", results.keyAt(0));
        assertEquals(null, results.valueAt(0));
    }

    public void testSkipLimit() throws Exception {
        Iterable<Item> store = seedDocuments(40);
        View view = new View("all", "function(doc,meta){emit(meta.id, null)}", null);
        Configuration config = new Configuration();
        config.setSkip(10);
        config.setLimit(5);
        QueryResult results = view.execute(store, config);

        assertEquals(40, results.getTotalRowCount());
        assertEquals(5, results.getFilteredRowCount());

        Map firstRow = (Map) results.rowAt(0);
        assertEquals("key-010", (String) firstRow.get("key"));
    }

    public void testRanging() throws Exception {
        Iterable<Item> store = seedDocuments(40);
        View view = new View("all", "function(doc){ emit( doc.val ); emit( doc.val+1 ); }", null);

        Configuration config = new Configuration();
        config.setStartKey(1);
        config.setEndKey(3);
        config.setInclusiveEnd(true);

        /* filter results: 1, 1, 2, 2, 3, 3 */
        QueryResult results = view.execute(store, config);
        assertEquals(80, results.getTotalRowCount());
        assertEquals(6, results.getFilteredRowCount());
        assertEquals(1, results.numKeyAt(0));
        assertEquals("key-000", results.idAt(0));

        /* filter results: 3, 3, 2, 2, 1, 1 */
        config.setDescending(true);
        config.setStartKey(3);
        config.setEndKey(1);

        results = view.execute(store, config);
        assertEquals(80, results.getTotalRowCount());
        assertEquals(6, results.getFilteredRowCount());
        assertEquals(3, results.numKeyAt(0));
        assertEquals("key-003", results.idAt(0));
    }

    public void testRangingExclusiveEnd() throws Exception {
        Iterable<Item> store = seedDocuments(40);
        View view = new View("all", "function(doc){emit(doc.val); emit(doc.val+1);}", null);

        Configuration config = new Configuration();
        config.setStartKey(1);
        config.setEndKey(3);
        config.setInclusiveEnd(false);

        QueryResult res = view.execute(store, config);
        assertEquals(80, res.getTotalRowCount());
        assertEquals(4, res.getFilteredRowCount());
    }

    public void testMapperDescending() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all", "function(doc,meta){emit(meta.id, null)}", null);
        Configuration config = new Configuration();
        config.setDescending(true);
        QueryResult res = view.execute(store, config);
        assertEquals(9, res.getFilteredRowCount());
        assertEquals("key-008", res.keyAt(0));
    }

    public void testMapperEmittingCustomKey() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all", "function(doc,meta){emit(meta.id.toUpperCase(), (doc.val + 1).toString())}", null);
        QueryResult results = view.execute(store);

        assertEquals(9, results.getFilteredRowCount());
        assertEquals("KEY-000", results.keyAt(0));
        assertEquals("key-000", results.idAt(0));
        assertEquals("1", results.valueAt(0));
        assertEquals(null, results.rowAt(0).get("doc"));
    }

    public void testReduceCount() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all",
                "function(doc,meta){emit(meta.id)}",
                "function(keys, values, rereduce){ return values.length; }");

        Configuration config = new Configuration();
        config.setReduce(true);

        QueryResult results = view.execute(store, config);
        assertEquals(1, results.getFilteredRowCount());
        assertEquals(null, results.keyAt(0));
        assertEquals(9, results.numValAt(0));
    }

    public void testReduceCountBuiltin() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all", "function(doc,meta){emit(meta.id)}", "_count");
        QueryResult results = view.execute(store);

        assertEquals(1, results.getFilteredRowCount());
        assertEquals(null, results.keyAt(0));
        assertEquals(9, results.numValAt(0));
    }

    public void testReduceSum() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all",
                "function(doc,meta){emit(meta.id, doc.val)}",
                "function(keys, values, rereduce){ return sum(values); }");

        QueryResult results = view.execute(store);
        assertEquals(1, results.getFilteredRowCount());
        assertEquals(null, results.keyAt(0));
        assertEquals(36, results.numValAt(0));
    }

    public void testReduceSumBuiltin() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all", "function(doc,meta){emit(meta.id, doc.val)}", "_sum");
        QueryResult results = view.execute(store);

        assertEquals(1, results.getFilteredRowCount());
        assertEquals(null, results.keyAt(0));
        assertEquals(36, results.numValAt(0));
    }

//    public void testReduceStatsBuiltin() throws Exception {
//        Iterable<Item> store = seedDocuments(9);
//        View view = new View("all", "function(doc){emit(meta.id, doc.val)}", "_stats");
//        HashMap results = view.execute(store);
//
//        ArrayList rows = (ArrayList) results.get("rows");
//        assertEquals(1, rows.size());
//
//        HashMap firstRow = (HashMap) rows.get(0);
//        assertEquals("null", firstRow.get("key").toString());
//        HashMap stats = (HashMap) firstRow.get("value");
//        assertEquals(36, ( (Number) stats.get("sum") ).intValue());
//        assertEquals(204, ( (Number) stats.get("sumsqr") ).intValue());
//        assertEquals(9, ( (Number) stats.get("count") ).intValue());
//        assertEquals(0, ( (Number) stats.get("min") ).intValue());
//        assertEquals(8, ( (Number) stats.get("max") ).intValue());
//    }
//

    @SuppressWarnings("unchecked")
    public void testReduceGroupCount() throws Exception {
        Iterable<Item> store = seedDocuments(9);
        View view = new View("all",
                "function(doc,meta){if (doc.val % 2 == 0) { emit([\"odd\", meta.id])} else { emit([\"even\", meta.id]) } }",
                "function(keys, values, rereduce){ return values.length; }");

        QueryResult results = view.execute(store);
        assertEquals(1, results.getFilteredRowCount());
        assertEquals(null, results.keyAt(0));
        assertEquals(9, results.numValAt(0));

        Configuration config = new Configuration();
        config.setGroup(true); //K=[e1,..en]
        results = view.execute(store, config);
        assertEquals(9, results.getFilteredRowCount());

        // Analyze row
        List<Object> ll = (List<Object>)results.keyAt(0);
        assertEquals(2, ll.size());
        assertEquals("even", ll.get(0));
        assertEquals("key-001", ll.get(1));
        assertEquals(1, results.numValAt(0));


        config = new Configuration();
        config.setGroupLevel(1); //K=[e1]
        results = view.execute(store, config);
        assertEquals(2, results.getFilteredRowCount());

        ll = (List<Object>) results.keyAt(0);
        assertEquals(1, ll.size());
        assertEquals("even", ll.get(0));
        assertEquals(4, results.numValAt(0));

        config = new Configuration();
        config.setGroupLevel(0); //K=null
        results = view.execute(store, config);

        assertEquals(1, results.getFilteredRowCount());
        assertEquals(null, results.keyAt(0));
        assertEquals(9, results.numValAt(0));

        config = new Configuration();
        config.setGroupLevel(2); // K=[e1, e2]
        results = view.execute(store, config);
        assertEquals(9, results.getFilteredRowCount());
        ll = (List<Object>) results.keyAt(0);
        assertEquals(2, ll.size());
        assertEquals("even", ll.get(0));
        assertEquals("key-001", ll.get(1));
        assertEquals(1, results.numValAt(0));
    }

    public void testItAllowsToTurnOffReduce() throws Exception {
        Iterable<Item> store = seedDocuments(40);
        View view = new View("all", "function(doc,meta){emit(meta.id, null)}", "_count");
        Configuration config = new Configuration();
        config.setReduce(false);

        QueryResult results = view.execute(store, config);
        assertEquals(40, results.getTotalRowCount());
        assertEquals(40, results.getFilteredRowCount());
        assertEquals("key-000", results.keyAt(0));
    }

    @SuppressWarnings("unchecked")
    public void testFilters() throws Exception {
        Iterable<Item> store = seedDocuments(40);
        View view = new View("all", "function(doc,meta){ emit([\"id\", meta.id], null); }");

        Configuration config = new Configuration();
        config.setEncodedKey("[ \"id\"   ,   \"key-001\" ]");

        QueryResult results = view.execute(store, config);
        assertEquals(40, results.getTotalRowCount());
        assertEquals(1, results.getFilteredRowCount());
        List<Object> ll = (List<Object>)results.keyAt(0);
        assertEquals(2, ll.size());
        assertEquals("id", ll.get(0));
        assertEquals("key-001", ll.get(1));

        config = new Configuration();
        List<String> keys = new ArrayList<String>();
        keys.add("[\"id\", \"key-006\"]");
        keys.add("[\"id\", \"key-008\"]");
        config.setEncodedKeys(keys);
        results = view.execute(store, config);

        assertEquals(40, results.getTotalRowCount());
        assertEquals(2, results.getFilteredRowCount());
        ll = (List<Object>) results.keyAt(0);
        assertEquals(2, ll.size());
        assertEquals("id", ll.get(0));
        assertEquals("key-006", ll.get(1));
    }
}
