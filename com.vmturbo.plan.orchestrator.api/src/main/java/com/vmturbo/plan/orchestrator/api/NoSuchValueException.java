package com.vmturbo.plan.orchestrator.api;

/**
 * Exception to be thrown when value is invalid.
 */
public class NoSuchValueException extends Exception {
    /**
     * Serial id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param message the error message.
     */
    public NoSuchValueException(String message) {
        super(message);
    }
}
