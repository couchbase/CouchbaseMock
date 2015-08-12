package org.couchbase.mock.views;

import java.util.List;
import java.util.Map;

/**
 * Query result object. This is returned from {@link View#execute(Iterable, Configuration)}, and
 * is primarily used for tests. To actually return a view result to the user, it is recommended
 * to use the {@link View#executeRaw(Iterable, Configuration)} method instead
 */
public class QueryResult {
    final Map<String,Object> raw;
    final List<Object> rows;
    QueryResult(Map<String,Object> raw) {
        this.raw = raw;
        this.rows = (List<Object>) raw.get("rows");
    }

    /**
     * @return The {@code total_rows} field showing how many total items were indexed
     */
    public int getTotalRowCount() {
        Number nn = (Number) raw.get("total_rows");
        return nn.intValue();
    }

    /**
     * @return The number of rows actually returned for the query
     */
    public int getFilteredRowCount() {
        return rows.size();
    }

    /**
     * Get the raw JSON row at a given index
     * @param ix The index of the row to fetch
     * @return The row
     */
    public Map<String,Object> rowAt(int ix) {
        return (Map<String,Object>) rows.get(ix);
    }

    /**
     * Returns the key for a row at a given index
     * @param ix The index of the row
     * @return A key. The user should cast this to the appropriate type as needed
     */
    public Object keyAt(int ix) {
        return rowAt(ix).get("key");
    }

    /** Like {@link #keyAt(int)} but casts the key as an integer */
    public int numKeyAt(int ix) {
        return ((Number)keyAt(ix)).intValue();
    }

    /**
     * Returns the <i>value</i> of a given row as a number.
     * @param ix The index of the row. The row must have a value and it must be numeric
     * @return The numeric value
     */
    public int numValAt(int ix) {
        return ((Number)valueAt(ix)).intValue();
    }

    /**
     * Returns the <i>ID</i> (document ID) of the row in the given index
     * @param ix The index of the row. The row must be a result of a non-reduce query
     * @return the document ID
     */
    public String idAt(int ix) {
        return (String)rowAt(ix).get("id");
    }

    /**
     * Returns the raw value for the row
     * @param ix The index of the row
     * @return The rae value
     */
    public Object valueAt(int ix) {
        return rowAt(ix).get("value");
    }
}
