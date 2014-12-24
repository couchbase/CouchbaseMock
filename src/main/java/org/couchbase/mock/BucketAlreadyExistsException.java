package org.couchbase.mock;

/**
 * Created by mnunberg on 12/24/14.
 */
public class BucketAlreadyExistsException extends Exception {
    public BucketAlreadyExistsException(String name) {
        super("Bucket : " +name + " already exists");
    }
}
