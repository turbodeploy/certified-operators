package com.vmturbo.identity.store;

/**
 * Checked exception indicating an error fetching information from the OID store.
 **/
public class IdentityStoreException extends Exception {
    public IdentityStoreException(String message) {
        super(message);
    }
    public IdentityStoreException(String message, Throwable t) {
        super(message, t);
    }
}
