package com.vmturbo.topology.processor.identity;

/**
 * Thrown if there is an error with OID assignment.
 */
public class IdentityProviderException extends Exception {

    public IdentityProviderException(String message) {
        super(message);
    }

    public IdentityProviderException(String message, Exception cause) {
        super(message, cause);
    }

}
