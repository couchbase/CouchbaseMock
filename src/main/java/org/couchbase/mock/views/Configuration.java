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

import java.util.List;

/**
 *
 * @author Sergey Avseyev
 */
public class Configuration {

    /* Include the full content of the documents in the return */
    private boolean includeDocs = false;
    /* Return the documents in descending by key order */
    private boolean descending = false;
    /* Stop returning records when the specified key is reached */
    private String end_key;
    /* Return records starting with the specified key */
    private String start_key;
    /* Stop returning records when the specified document ID is reached */
    private String end_key_doc_id;
    /* Return records starting with the specified document ID */
    private String start_key_doc_id;
    /* Group the results using the reduce function to a group or single row */
    private boolean group = false;
    /* Specify the group level to be used */
    private int group_level;
    /* Timeout before the view request is dropped */
    private int connection_timeout;
    /* Specifies whether the specified end key should be included in the result */
    private boolean inclusive_end = true;
    /* Return only documents that match the specified key */
    private String key;
    /* Return only documents that match the specified keys */
    private List keys;
    /* Limit the number of the returned documents to the specified number */
    private Integer limit = null;
    /* Sets the response in the event of an error.
     * Possible values:
     * * continue   Continue to generate view information in the event of an
     *              error, including the error information in the view response
     *              stream.
     * 
     * * stop       Stop immediately when an error condition occurs. No further
     *              view information will be returned.
     */
    private String on_error = "continue";
    /* Use the reduction function */
    private boolean reduce = true;
    /* Skip this number of records before starting to return the results */
    private int skip = 0;
    /* Allow the results from a stale view to be used.
     * Possible values:
     * * false          Force a view update before returning data
     * * ok	            Allow stale views
     * * update_after   Allow stale view, update view after it has been accessed
     */
    private String stale = "update_after";

    public boolean includeDocs() {
        return includeDocs;
    }

    public boolean isDescending() {
        return descending;
    }

    public void setIncludeDocs(boolean includeDocs) {
        this.includeDocs = includeDocs;
    }

    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    public String getEndKey() {
        return end_key;
    }

    public void setEndKey(String endkey) {
        this.end_key = endkey;
    }

    public String getStartKey() {
        return start_key;
    }

    public void setStartKey(String startkey) {
        this.start_key = startkey;
    }

    public boolean hasRange() {
        return start_key != null || start_key_doc_id != null
                || end_key != null || end_key_doc_id != null;
    }

    public String getEndKeyDocId() {
        return end_key_doc_id;
    }

    public void setEndKeyDocId(String endkey_docid) {
        this.end_key_doc_id = endkey_docid;
    }

    public String getStartKeyDocId() {
        return start_key_doc_id;
    }

    public void setStartKeyDocId(String startkey_docid) {
        this.start_key_doc_id = startkey_docid;
    }

    public boolean isGroup() {
        return group;
    }

    public void setGroup(boolean group) {
        this.group = group;
    }

    public int getGroupLevel() {
        return group_level;
    }

    public void setGroupLevel(int group_level) {
        this.group_level = group_level;
    }

    public int getConnectionTimeout() {
        return connection_timeout;
    }

    public void setConnectionTimeout(int connection_timeout) {
        this.connection_timeout = connection_timeout;
    }

    public boolean isInclusiveEnd() {
        return inclusive_end;
    }

    public void setInclusiveEnd(boolean inclusive_end) {
        this.inclusive_end = inclusive_end;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getOnError() {
        return on_error;
    }

    public void setOnError(String on_error) {
        this.on_error = on_error;
    }

    public boolean reduce() {
        return reduce;
    }

    public void setReduce(boolean reduce) {
        this.reduce = reduce;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public String getStale() {
        return stale;
    }

    public void setStale(String stale) {
        this.stale = stale;
    }

    public List getKeys() {
        return keys;
    }

    public void setKeys(List keys) {
        this.keys = keys;
    }
}
