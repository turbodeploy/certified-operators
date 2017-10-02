package com.vmturbo.topology.processor.identity.storage;

/**
 * Exception thrown when there is an error interacting with the database to store or retrieve
 * identity information.
 */
public class IdentityDatabaseException extends Exception {

    public IdentityDatabaseException(final Throwable cause) {
        super(cause);
    }

}
