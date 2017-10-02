package com.vmturbo.topology.processor.identity;

/**
 * Exception thrown when the Identity system is uninitialized - i.e. when the already-assigned
 * IDs have not been retrieved from the database.
 */
public class IdentityUninitializedException extends Exception {

    public IdentityUninitializedException() {
        super("Identity service not initialized!");
    }

}
