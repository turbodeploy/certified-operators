package com.vmturbo.identity.exceptions;

/**
 * Top level exception of identity package. The other kind of identity exceptions should extend from it.
 **/
public class IdentityServiceException extends Exception {

    public IdentityServiceException(String message) {
        super(message);
    }
    public IdentityServiceException(String message, Throwable t) {
        super(message, t);
    }
}
