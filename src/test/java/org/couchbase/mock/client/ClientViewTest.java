/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package org.couchbase.mock.client;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.internal.HttpCompletionListener;
import com.couchbase.client.internal.HttpFuture;
import com.couchbase.client.protocol.views.*;
import com.couchbase.client.protocol.views.ViewOperation.ViewCallback;
import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.couchbase.mock.Bucket;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.JsonUtils;
import org.junit.*;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Verifies the correct functionality of views.
 */
public class ClientViewTest {

    static {
        ClientBaseTest.initJCBCEnv();
    }

    /**
     * The time to wait until persistence writing is done. This setting should
     * be slightly higher than normal to make sure no timeouts occur, even when
     * adding and removing design documents on slower machines.
     */
    private static final int PERSIST_WAIT_TIME = 60;
    protected static CouchbaseMock mock = null;
    protected static CouchbaseClient client = null;
    protected static BucketConfiguration config;
    private static final Map<String, Object> ITEMS;

    public static final String DESIGN_DOC_W_REDUCE = "doc_with_view";
    public static final String DESIGN_DOC_WO_REDUCE = "doc_without_view";
    public static final String DESIGN_DOC_BINARY = "doc_binary";
    public static final String VIEW_NAME_W_REDUCE = "view_with_reduce";
    public static final String VIEW_NAME_WO_REDUCE = "view_without_reduce";
    public static final String VIEW_NAME_FOR_DATED = "view_emitting_dated";
    public static final String VIEW_NAME_BINARY = "view_binary";

    static {
        ITEMS = new HashMap<String, Object>();
        int d = 0;
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                for (int k = 0; k < 5; k++, d++) {
                    String type = new String(new char[] { (char) ('f' + i) });
                    String small = (new Integer(j)).toString();
                    String large = (new Integer(k)).toString();
                    String doc = generateDoc(type, small, large);
                    ITEMS.put("key" + d, doc);
                }
            }
        }
    }

    protected static void initClient() throws Exception {
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        CouchbaseConnectionFactory cf;
        List<URI> uriList = new ArrayList<URI>();
        uriList.add(new URI("http", null, "localhost", mock.getHttpPort(), "/pools", "", ""));
        cf = cfb.buildCouchbaseConnection(uriList, config.name, config.password);
        client = new CouchbaseClient(cf);
    }

    protected static void clearAllBuckets() throws Exception {
        Collection<Bucket> buckets = mock.getBuckets().values();
        for (Bucket bucket : buckets) {
            mock.removeBucket(bucket.getName());
        }
    }

    protected static void createDesignDocument(String name, String ddoc) throws Exception {
        Map<String,Object> mm = JsonUtils.decodeAsMap(ddoc);
        mm.put("id", "_design/" + name);
        String ss = JsonUtils.encode(mm);
        RestAPIUtil.defineDesignDocument(mock, name, ss, "default");
    }
    protected static void deleteDesignDocument(String name) throws Exception {
        RestAPIUtil.deleteDeignDocument(mock, name, "default");
    }

    @BeforeClass
    public static void before() throws Exception {
        config = new BucketConfiguration();
        config.numNodes = 4;
        config.numReplicas = 1;
        config.name = "default";
        config.password = "";

        List<BucketConfiguration> configs = new ArrayList<BucketConfiguration>();
        configs.add(config);

        if (mock == null) {
            mock = new CouchbaseMock(null, 0, 4, 0, 1024, "", 1);
            mock.clearInitialConfigs();
            mock.start();
            mock.waitForStartup();
        } else {
            clearAllBuckets();
        }

        mock.createBucket(config);

        initClient();

        String view = "{\"language\":\"javascript\",\"views\":{\""
                + VIEW_NAME_W_REDUCE + "\":{\"map\":\"function (doc) { "
                + "if(doc.type != \\\"dated\\\") {emit(doc.type, 1)}}\","
                + "\"reduce\":\"_sum\" }}}";
        try {
            createDesignDocument(DESIGN_DOC_W_REDUCE, view);
        } catch (IOException ex) {
            throw ex;
        }

        // Creating the Design/View for the binary docs

        view = "{\"language\":\"javascript\",\"views\":{\""
                + VIEW_NAME_BINARY + "\":{\"map\":\"function (doc, meta) "
                +"{ if(meta.id.match(/nonjson/)) { emit(meta.id, null); }}\"}}}";
        createDesignDocument(DESIGN_DOC_BINARY, view);

        view = "{\"language\":\"javascript\",\"views\":{\""
                + VIEW_NAME_FOR_DATED + "\":{\"map\":\"function (doc) {  "
                + "emit(doc.type, 1)}\"}}}";
        createDesignDocument(DESIGN_DOC_WO_REDUCE, view);

        for (Entry<String, Object> item : ITEMS.entrySet()) {
            OperationFuture<Boolean> future = client.set(item.getKey(), item.getValue());
            assertTrue(future.getStatus().toString(), future.get());
        }
        client.shutdown();
    }

    @Before
    public void beforeTest() throws Exception {
        initClient();
    }

    /**
     * Shuts the client down and nulls the reference.
     *
     * @throws Exception
     */
    @After
    public void afterTest() throws Exception {
        client.shutdown();
        client = null;
    }

    @AfterClass
    public static void after() throws Exception {
        // Delete all design documents I created
        deleteDesignDocument(DESIGN_DOC_W_REDUCE);
        deleteDesignDocument(DESIGN_DOC_WO_REDUCE);
        deleteDesignDocument(DESIGN_DOC_BINARY);
    }

    private static String generateDoc(String type, String small, String large) {
        return "{\"type\":\"" + type + "\"" + ",\"small range\":\"" + small + "\","
                + "\"large range\":\"" + large + "\"}";
    }

    private static String generateDatedDoc(int year, int month, int day) {
        return "{\"type\":\"dated\",\"year\":" + year + ",\"month\":" + month + ","
                + "\"day\":" + day + "}";
    }

    /**
     * Tests the view query with docs i.e. includeDocs and no reduce.
     *
     * @pre Retrieve a view including docs from the client.
     *    Perform an async query on the view.
     * @post Assert row id and document id if successful.
     */
    @Test
    public void testQueryWithDocs() {
        Query query = new Query();
        query.setReduce(false);
        query.setIncludeDocs(true);
        query.setStale(Stale.FALSE);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        assert view != null : "Could not retrieve view";
        HttpFuture<ViewResponse> future = client.asyncQuery(view, query);
        ViewResponse response=null;
        try {
            response = future.get();
        } catch (ExecutionException ex) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
        }
        assert future.getStatus().isSuccess() : future.getStatus();

        Iterator<ViewRow> itr = response.iterator();
        while (itr.hasNext()) {
            ViewRow row = itr.next();
            if (ITEMS.containsKey(row.getId())) {
                assert ITEMS.get(row.getId()).equals(row.getDocument());
            }
        }
        assert ITEMS.size() == response.size() : future.getStatus().getMessage();
    }

    /**
     * Tests the view query without includeDocs and reduce.
     *
     * @pre Retrieve a view from the client.
     *   Perform an async query on the view.
     * @post Assert status and the response size.
     * @throws Exception
     */
    @Test
    public void testViewNoDocs() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future = client.asyncQuery(view, query);
        assert future.getStatus().isSuccess() : future.getStatus();
        ViewResponse response = future.get();

        Iterator<ViewRow> itr = response.iterator();
        while (itr.hasNext()) {
            ViewRow row = itr.next();
            if (!ITEMS.containsKey(row.getId())) {
                assert false : ("Got an item that I shouldn't have gotten.");
            }
        }
        assert response.size() == ITEMS.size() : future.getStatus();
    }

    @Test
    public void testViewQueryWithListener() throws Exception {
        final Query query = new Query();
        query.setReduce(false);

        HttpFuture<View> future =
                client.asyncGetView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger callCount = new AtomicInteger(0);
        future.addListener(new HttpCompletionListener() {
            @Override
            public void onComplete(HttpFuture<?> f) throws Exception {
                View view = (View) f.get();
                HttpFuture<ViewResponse> queryFuture = client.asyncQuery(view, query);
                queryFuture.addListener(new HttpCompletionListener() {
                    @Override
                    public void onComplete(HttpFuture<?> f) throws Exception {
                        ViewResponse resp = (ViewResponse) f.get();
                        if (resp.size() == ITEMS.size()) {
                            callCount.incrementAndGet();
                            latch.countDown();
                        }
                    }
                });
            }
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(1, callCount.get());
    }

    @Test
    public void testViewFutureWithListener() throws Exception {
        final Query query = new Query();
        query.setReduce(false);
        query.setIncludeDocs(true);

        HttpFuture<View> future =
                client.asyncGetView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);

        final CountDownLatch latch = new CountDownLatch(1);
        future.addListener(new HttpCompletionListener() {
            @Override
            public void onComplete(HttpFuture<?> f) throws Exception {
                View view = (View) f.get();
                HttpFuture<ViewResponse> queryFuture = client.asyncQuery(view, query);
                queryFuture.addListener(new HttpCompletionListener() {
                    @Override
                    public void onComplete(HttpFuture<?> f) throws Exception {
                        ViewResponse resp = (ViewResponse) f.get();
                        if (resp.size() == ITEMS.size()) {
                            latch.countDown();
                        }
                    }
                });
            }
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Tests the view query with reduce functionality.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Iterate over the reduced result set and
     *   assert the key value and size of the returned results.
     * @throws Exception
     */
    @Test
    public void testReduce() throws Exception {
        Query query = new Query();
        query.setReduce(true);
        query.setStale(Stale.FALSE);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query);
        ViewResponse reduce = future.get();

        Iterator<ViewRow> itr = reduce.iterator();
        while (itr.hasNext()) {
            ViewRow row = itr.next();
            assertNull(row.getKey());
            assertEquals(ITEMS.size(), Integer.parseInt(row.getValue()));
        }
    }

    /**
     * Tests the view query with implicit reduce.
     *
     * @pre Retrieve a view from the client. Perform an async
     *    query on the view. When a view with reduce is selected,
     *    make sure that implicitly reduce is used to align with
     *    the UI behaviour.
     * @post  Iterate over the reduced result set and assert
     *    the key value and size of the returned results.
     */
    @Test
    public void testImplicitReduce() {
        Query query = new Query();
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        ViewResponse reduce = client.query(view, query);
        Iterator<ViewRow> iterator = reduce.iterator();
        while(iterator.hasNext()) {
            ViewRow row = iterator.next();
            assertNull(row.getKey());
            assertEquals(ITEMS.size(), Integer.parseInt(row.getValue()));
        }
    }

    /**
     * Tests the view query with query set descending.
     *
     * @pre Retrieve a view from the client.
     *   Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetDescending() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setDescending(true));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with last document id.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetEndKeyDocID() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future = client.asyncQuery(view, query.setEndkeyDocID("an_id"));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with grouping true.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetGroup() throws Exception {
        Query query = new Query();
        query.setReduce(true);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setGroup(true));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with grouping true and without reduce.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  InvalidViewException will be returned.
     * @throws Exception
     */
    @Test(expected = InvalidViewException.class)
    public void testQuerySetGroupNoReduce() throws Exception {
        Query query = new Query();
        query.setGroup(true);
        View view = client.getView(DESIGN_DOC_WO_REDUCE, VIEW_NAME_WO_REDUCE);
        client.asyncQuery(view, query).get();
    }

    /**
     * Tests the view query with group level as 1.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetGroupWithLevel() throws Exception {
        Query query = new Query();
        query.setReduce(true);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setGroupLevel(1));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with last result set included.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetInclusiveEnd() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setInclusiveEnd(true));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with a key id set. It will return
     *    only documents that match the specified key.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetKey() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setKey("a_key"));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with limit as 10,
     *    to return only 10 documents.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     * @TODO Inspect the correctness of the limit.
     */
    @Test
    public void testQuerySetLimit() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setLimit(10));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with start and end key values.
     *    Returns records in the given key range.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetRange() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setRange("key0", "key2"));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with start key. Return records
     *    with a value equal to or greater than the specified key.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetRangeStart() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setRangeStart("start"));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with complex key as the starting key.
     *
     * @pre  Prepare a complex query with date as the criteria.
     *       Retrieve a view from the client.
     *       Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetRangeStartComplexKey() throws Exception {

        // create a mess of stuff to query
        for (int i = 2009; i<2013; i++) {
            for (int j = 1; j<13; j++) {
                for (int k = 1; k<32; k++) {
                    client.add("date" + i + j + k, 600, generateDatedDoc(i, j, k));
                }
            }
        }

        // now query it
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setRangeStart(ComplexKey.of(2012, 9, 5)));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with last key. Stops returning
     *    records when the specified key is reached.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetRangeEnd() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setRangeEnd("end"));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query with a skip number so as to skip
     *    that many records before starting to return the results.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetSkip() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setSkip(0));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query which allows the
     *    results from a stale view to be used.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetStale() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setStale(Stale.OK));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the view query by setting the start
     *    key doc id to return records starting with it.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetStartkeyDocID() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setStartkeyDocID("key0"));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Tests the query with OnError parameter set.
     *    Sets the response in the event of an error.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testQuerySetOnError() throws Exception {
        Query query = new Query();
        query.setReduce(false);
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setOnError(OnError.CONTINUE));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    @Test
    public void testViewLoadWithListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        client.asyncGetView(DESIGN_DOC_WO_REDUCE, VIEW_NAME_W_REDUCE).addListener(
                new HttpCompletionListener() {
                    @Override
                    public void onComplete(HttpFuture<?> httpFuture) throws Exception {
                        if (httpFuture.getStatus().isSuccess()) {
                            latch.countDown();
                        }
                    }
                });
        assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    /**
     * Tests the query with reduce as true but not set.
     *
     * @pre Retrieve a view from the client.
     *    Perform an async query on the view.
     * @post  InvalidViewException is caught and
     *    the query happens without reduce.
     * @throws Exception
     */
    @Test
    public void testReduceWhenNoneExists() throws Exception {
        Query query = new Query();
        query.setReduce(true);
        try {
            View view = client.getView(DESIGN_DOC_WO_REDUCE, VIEW_NAME_WO_REDUCE);
            client.asyncQuery(view, query);
        } catch (InvalidViewException e) {
            return; // Pass, no reduce exists.
        }
        assert false : ("No view exists and this query still happened");
    }

    /**
     * Tests the query with complex key of range end.
     *
     * @pre Retrieve a view from the client. Perform an async query on the view.
     * @post Assert the response status is not null.
     * @throws Exception
     */
    @Test
    public void testComplexKeyQuery() throws Exception {
        Query query = new Query();
        query.setReduce(false);

        ComplexKey rangeEnd = ComplexKey.of("end");
        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        HttpFuture<ViewResponse> future =
                client.asyncQuery(view, query.setRangeEnd(rangeEnd));
        ViewResponse response = future.get();
        assert response != null : future.getStatus();
    }

    /**
     * Test view with docs with errors. It tries to
     * ensure that the client does not crash when receiving a
     * bad HTTP response.
     *
     * @pre Prepare a new view and instantiate a new Http
     * NoDocs operation on the same.Verify the Http Response
     * having the rows array as empty.
     * @post Validates the Http Operation as successful if
     * the view has got data and request reaches the server.
     * @throws Exception
     */
    @Test
    public void testViewDocsWithErrors() throws Exception {
        View view = new View("a", "b", "c", true, true);
        HttpOperation op = new DocsOperationImpl(null, view, new ViewCallback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                assert status.isSuccess();
            }

            @Override
            public void complete() {
                // Do nothing
            }

            @Override
            public void gotData(ViewResponse response) {
                assert response.getErrors().size() == 2;
                Iterator<RowError> row = response.getErrors().iterator();
                assert row.next().getFrom().equals("127.0.0.1:5984");
                assert response.size() == 0;
            }
        });
        HttpResponse response =
                new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "");
        String entityString = "{\"total_rows\":0,\"rows\":[],\"errors\": [{\"from"
                + "\":\"127.0.0.1:5984\",\"reason\":\"Design document `_design/test"
                + "foobar` missing in database `test_db_b`.\"},{\"from\":\"http://"
                + "localhost:5984/_view_merge/\",\"reason\":\"Design document `"
                + "_design/testfoobar` missing in database `test_db_c`.\"}]}";
        StringEntity entity = new StringEntity(entityString);
        response.setEntity(entity);
        op.handleResponse(response);
    }

    /**
     * Test view no docs with errors. It tries to
     * ensure that the client does not crash when receiving a
     * bad HTTP response.
     *
     * @pre Prepare a new view and instantiate a new Http
     * NoDocs operation on the same. Verify the Http Response
     * having the rows array as empty.
     * @post Validates the Http Operation as successful if
     * the view has got data and request reaches the server.
     * @throws Exception
     */
    @Test
    public void testViewNoDocsWithErrors() throws Exception {
        View view = new View("a", "b", "c", true, true);
        HttpOperation op = new NoDocsOperationImpl(null, view, new ViewCallback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                assert status.isSuccess();
            }

            @Override
            public void complete() {
                // Do nothing
            }

            @Override
            public void gotData(ViewResponse response) {
                assert response.getErrors().size() == 2;
                Iterator<RowError> row = response.getErrors().iterator();
                assert row.next().getFrom().equals("127.0.0.1:5984");
                assert response.size() == 0;
            }
        });
        HttpResponse response =
                new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "");
        String entityString = "{\"total_rows\":0,\"rows\":[],\"errors\": [{\"from"
                + "\":\"127.0.0.1:5984\",\"reason\":\"Design document `_design/test"
                + "foobar` missing in database `test_db_b`.\"},{\"from\":\"http://"
                + "localhost:5984/_view_merge/\",\"reason\":\"Design document `"
                + "_design/testfoobar` missing in database `test_db_c`.\"}]}";
        StringEntity entity = new StringEntity(entityString);
        response.setEntity(entity);
        op.handleResponse(response);
    }

    /**
     * Test view reduced with errors.
     *
     * @pre Prepare a new view and instantiate
     * a new Http Reduce operation on the same.
     * Verify the Http Response for the same.
     * @post Validates the Http Operation as
     * successful if the view has got data
     * and request reaches the server.
     * @throws Exception
     */
    @Test
    public void testViewReducedWithErrors() throws Exception {
        View view = new View("a", "b", "c", true, true);
        HttpOperation op = new ReducedOperationImpl(null, view, new ViewCallback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                assert status.isSuccess();
            }

            @Override
            public void complete() {
                // Do nothing
            }

            @Override
            public void gotData(ViewResponse response) {
                assert response.getErrors().size() == 2;
                Iterator<RowError> row = response.getErrors().iterator();
                assert row.next().getFrom().equals("127.0.0.1:5984");
                assert response.size() == 0;
            }
        });
        HttpResponse response =
                new BasicHttpResponse(HttpVersion.HTTP_1_1, 200, "");
        String entityString = "{\"total_rows\":0,\"rows\":[],\"errors\": [{\"from"
                + "\":\"127.0.0.1:5984\",\"reason\":\"Design document `_design/test"
                + "foobar` missing in database `test_db_b`.\"},{\"from\":\"http://"
                + "localhost:5984/_view_merge/\",\"reason\":\"Design document `"
                + "_design/testfoobar` missing in database `test_db_c`.\"}]}";
        StringEntity entity = new StringEntity(entityString);
        response.setEntity(entity);
        op.handleResponse(response);
    }


    /**
     * This test case adds two non-JSON documents and
     * utilises a special view that returns them.
     *
     * @pre Create non-JSON documents and set them to the db.
     * Prepare a view query with docs and iterate over the response.
     * Find the non json documents in the result set and assert them.
     * @post This makes sure that the view handlers don't break when
     * non-JSON data is read from the view.
     */
    @Test
    public void testViewWithBinaryDocs() throws Exception {
        // Create non-JSON documents
        Date now = new Date();
        client.set("nonjson1", 0, now, PersistTo.MASTER)
                .get(PERSIST_WAIT_TIME, TimeUnit.SECONDS);
        client.set("nonjson2", 0, 42, PersistTo.MASTER)
                .get(PERSIST_WAIT_TIME, TimeUnit.SECONDS);

        View view = client.getView(DESIGN_DOC_BINARY, VIEW_NAME_BINARY);
        Query query = new Query();
        query.setIncludeDocs(true);
        query.setReduce(false);
        query.setStale(Stale.FALSE);

        assert view != null : "Could not retrieve view";
        ViewResponse response = client.query(view, query);

        Iterator<ViewRow> itr = response.iterator();
        while (itr.hasNext()) {
            ViewRow row = itr.next();
            if(row.getKey().equals("nonjson1")) {
                assertEquals(now.toString(), row.getDocument().toString());
            }
            if(row.getKey().equals("nonjson2")) {
                assertEquals(42, row.getDocument());
            }
        }

        /* FIXME: I have no idea how the tests here ever worked properly, since
           these documents are not deleted afterwards. Here' I'm going to delete
           them so bad things don't happen to the tets.
        */
        client.delete("nonjson1").get();
        client.delete("nonjson2").get();
    }

    @Test
    public void testTotalNumRowsWithDocs() {
        Query query = new Query();
        query.setReduce(false).setIncludeDocs(true).setStale(Stale.FALSE);

        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        ViewResponse response = client.query(view, query);
        long totalRows = response.getTotalRows();
        assertTrue(ITEMS.size() <= totalRows);

        query.setLimit(5);
        response = client.query(view, query);
        totalRows = response.getTotalRows();
        assertTrue(ITEMS.size() <= totalRows);
    }

    @Test
    public void testTotalNumRowsWithoutDocs() {
        Query query = new Query();
        query.setReduce(false).setIncludeDocs(false).setStale(Stale.FALSE);

        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        ViewResponse response = client.query(view, query);
        long totalRows = response.getTotalRows();
        assertTrue(ITEMS.size() <= totalRows);

        query.setLimit(5);
        response = client.query(view, query);
        totalRows = response.getTotalRows();
        assertTrue(ITEMS.size() <= totalRows);
    }

    @Test(expected = IllegalStateException.class)
    public void testTotalNumRowsReduced() {
        Query query = new Query();
        query.setIncludeDocs(true).setStale(Stale.FALSE);

        View view = client.getView(DESIGN_DOC_W_REDUCE, VIEW_NAME_W_REDUCE);
        ViewResponse response = client.query(view, query);
        response.getTotalRows();
    }

    /**
     * This tests the design document creation using
     * views and spatial views.
     *
     * @pre Create two array lists with views and spatial views.
     * Using these, prepare a design document object. Pass this
     * instance to call the method asyncCreateDesignDoc on the
     * client. Put the current thread to sleep for 2000ms and then
     * again query the client for the just created design document.
     * @post Asserts true if the size of views in the design document is 2.
     */
    @Test
    public void testDesignDocumentCreation() throws InterruptedException {
        List<ViewDesign> views = new ArrayList<ViewDesign>();
        List<SpatialViewDesign> spviews = new ArrayList<SpatialViewDesign>();

        ViewDesign view1 = new ViewDesign(
                "view1",
                "function(a, b) {}"
        );
        views.add(view1);

        ViewDesign view2 = new ViewDesign(
                "view2",
                "function(b, c) {}",
                "function(red) {}"
        );
        views.add(view2);

        SpatialViewDesign spview = new SpatialViewDesign(
                "spatialfoo",
                "function(map) {}"
        );
        spviews.add(spview);

        DesignDocument doc = new DesignDocument("mydesign", views, spviews);
        HttpFuture<Boolean> result;
        boolean success = true;
        try {
            result = client.asyncCreateDesignDoc(doc);
            assertTrue(result.get());
        } catch (Exception ex) {
            ex.printStackTrace();
            success = false;
        }
        assertTrue(success);

        DesignDocument design = client.getDesignDoc("mydesign");
        assertEquals(2, design.getViews().size());
    }

    /**
     * This tests the design document creation using views.
     *
     * @pre Create an array list with views. Using this, prepare a
     * design document object. Pass this instance to call the method
     * asyncCreateDesignDoc on the client. Put the current thread to
     * sleep for 2000ms and then again query the client for the just
     * created design document.
     * @post Asserts true if the size of views in the design document is 1.
     */
    @Test
    public void testRawDesignDocumentCreation() throws InterruptedException {
        List<ViewDesign> views = new ArrayList<ViewDesign>();

        ViewDesign view = new ViewDesign(
                "viewname",
                "function(a, b) {}"
        );
        views.add(view);

        DesignDocument doc = new DesignDocument("rawdesign", views, null);
        HttpFuture<Boolean> result;
        boolean success = true;
        try {
            result = client.asyncCreateDesignDoc(doc.getName(), doc.toJson());
            assertTrue(result.get());
        } catch (Exception ex) {
            success = false;
        }
        assertTrue(success);

        DesignDocument design = client.getDesignDoc("rawdesign");
        assertEquals(1, design.getViews().size());
    }

    /**
     * Test invalid design doc handling.
     *
     * @pre pass any string to retrieve the views from it.
     * @post Return the InvalidViewException.
     */
    @Test
    public void testInvalidDesignDocumentCreation() throws Exception {
        String content = "{certainly_not_a_view: true}";
        HttpFuture<Boolean> result = client.asyncCreateDesignDoc("invalid_design", content);
        assertFalse(result.get());

        boolean success = false;
        try {
            client.getDesignDoc("invalid_design");
        } catch(InvalidViewException ex) {
            success = true;
        }
        assertTrue(success);
    }

    /**
     * This tests the design document deletion.
     *
     * @pre Create a design document object with the name
     * of the design document previously created. Asserts true
     * if the size of views in the design document is 2. Call the
     * method asyncDeleteDesignDoc on the client to delete this
     * existing design document. Put the current thread to sleep
     * for 2000ms and then again query the client for the just
     * deleted design document.
     * @post Asserts true for demonstrating the success
     * of the deletion operation.
     */
    @Test
    public void testDesignDocumentDeletion() throws InterruptedException {
        testDesignDocumentCreation();
        DesignDocument design = client.getDesignDoc("mydesign");
        assertEquals(2, design.getViews().size());

        boolean success = true;

        try {
            HttpFuture<Boolean> result = client.asyncDeleteDesignDoc("mydesign");
            assertTrue(result.get());
        } catch (Exception ex) {
            success = false;
        }
        assertTrue(success);

        success = false;
        try {
            design = client.getDesignDoc("mydesign");
        } catch(InvalidViewException e) {
            success = true;
        }
        assertTrue(success);
    }

    /**
     * Test invalid view handling.
     *
     * @pre pass any string in the view name and the design
     * doc name to retrieve the database view from it.
     * @post Return the InvalidViewException.
     */
    @Test(expected=InvalidViewException.class)
    public void testInvalidViewHandling() {
        String designDoc = "invalid_design";
        String viewName = "invalid_view";
        View view = client.getView(designDoc, viewName);
        assertNull(view);
    }

    /**
     * Test invalid view on valid design doc.
     */
    @Test(expected=InvalidViewException.class)
    public void testInvalidViewOnValidDesignDoc() {
        View view = client.getView(DESIGN_DOC_W_REDUCE, "invalidViewName");
    }

    /**
     * This test tries to retrieve the design document
     * with an invalid name.
     *
     * @pre Use an invalid name for search a design document
     * in the server. Call getDesignDocument method.
     * @post The design document is not loaded and the test
     * passes if InvalidViewException is returned as expected.
     */
    @Test(expected=InvalidViewException.class)
    public void testInvalidDesignDocHandling() {
        String designDoc = "invalid_design";
        client.getDesignDoc(designDoc);
    }
}