package com.vmturbo.topology.processor.identity;

/**
 * Thrown if there is an error with OID assignment.
 */
public class IdentityProviderException extends RuntimeException {

    public IdentityProviderException(String message, Exception cause) {
        super(message, cause);
    }

}
