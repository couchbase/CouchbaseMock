package org.couchbase.mock.views;

public class QueryExecutionException extends Exception {
    private final String exMessage;
    QueryExecutionException(String message) {
        super(message);
        this.exMessage = message;
    }
    public String getJsonString() {
        return exMessage;
    }
}