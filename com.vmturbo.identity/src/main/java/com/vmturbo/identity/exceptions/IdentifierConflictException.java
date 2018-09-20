package com.vmturbo.identity.exceptions;

/**
 * Checked exception indicating an error that the identity attributes need to be inserted or updated in
 * persistent store is already exist and mapping to another OID. We do not accept two rows with the same
 * identity attributes.
 **/
public class IdentifierConflictException extends IdentityServiceException {
    public IdentifierConflictException(String message) {
        super(message);
    }
    public IdentifierConflictException(String message, Throwable t) {
        super(message, t);
    }
}
