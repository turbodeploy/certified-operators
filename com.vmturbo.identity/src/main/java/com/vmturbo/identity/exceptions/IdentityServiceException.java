package com.vmturbo.identity.exceptions;

/**
 * Top level exception of identity package. The other kind of identity exceptions should extend from
 * it.
 **/
public class IdentityServiceException extends Exception {

    /**
     * Constructs identity service exception.
     *
     * @param message details message
     */
    public IdentityServiceException(String message) {
        super(message);
    }

    /**
     * Constructs identity service exception.
     *
     * @param message details message
     * @param t root cause
     */
    public IdentityServiceException(String message, Throwable t) {
        super(message, t);
    }
}
