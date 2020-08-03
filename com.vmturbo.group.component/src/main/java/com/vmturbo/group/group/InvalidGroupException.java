package com.vmturbo.group.group;

/**
 * Exception to be thrown if group related request is not valid for the existing group.
 */
public class InvalidGroupException extends Exception {
    /**
     * Constructs an exception with a message.
     *
     * @param message exception message
     */
    public InvalidGroupException(String message) {
        super(message);
    }

    /**
     * Constracts an exception with a message and a root cause.
     *
     * @param message exception message
     * @param cause root cause of the exception
     */
    public InvalidGroupException(String message, Throwable cause) {
        super(message, cause);
    }
}
