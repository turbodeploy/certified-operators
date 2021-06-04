package com.vmturbo.group.common;

/**
 * An exception to indicate that the user provided incorrect input.
 */
public class InvalidParameterException extends Exception {

    /**
     * Constructor.
     *
     * @param message description of the error.
     */
    public InvalidParameterException(final String message) {
        super(message);
    }
}
