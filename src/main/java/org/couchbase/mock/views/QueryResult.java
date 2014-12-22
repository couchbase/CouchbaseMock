package org.couchbase.mock.views;

import java.util.List;
import java.util.Map;

public class QueryResult {
    final Map<String,Object> raw;
    final List<Object> rows;
    QueryResult(Map<String,Object> raw) {
        this.raw = raw;
        this.rows = (List<Object>) raw.get("rows");
    }

    public int getTotalRowCount() {
        Number nn = (Number) raw.get("total_rows");
        return nn.intValue();
    }

    public int getFilteredRowCount() {
        return rows.size();
    }

    public Map<String,Object> rowAt(int ix) {
        return (Map<String,Object>) rows.get(ix);
    }

    public Object keyAt(int ix) {
        return rowAt(ix).get("key");
    }

    public int numKeyAt(int ix) {
        return ((Number)keyAt(ix)).intValue();
    }
    public int numValAt(int ix) {
        return ((Number)valueAt(ix)).intValue();
    }

    public String idAt(int ix) {
        return (String)rowAt(ix).get("id");
    }

    public Object valueAt(int ix) {
        return rowAt(ix).get("value");
    }
}
