package com.vmturbo.identity.exceptions;

/**
 * Checked exception indicating an error fetching information from the OID store.
 **/
public class IdentityStoreException extends IdentityServiceException {
    public IdentityStoreException(String message) {
        super(message);
    }

    public IdentityStoreException(String message, Throwable t) {
        super(message, t);
    }
}
