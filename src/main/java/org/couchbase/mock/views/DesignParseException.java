package org.couchbase.mock.views;

/**
* Created by mnunberg on 12/18/14.
*/
public class DesignParseException extends Exception {
    public DesignParseException(Throwable caught) {
        super(caught);
    }
    public DesignParseException(String s) {
        super(s);
    }
}
