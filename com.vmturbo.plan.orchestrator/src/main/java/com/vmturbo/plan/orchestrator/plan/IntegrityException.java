package com.vmturbo.plan.orchestrator.plan;

/**
 * Exception to throw when some referential integrity checks failed.
 */
public class IntegrityException extends Exception {
    public IntegrityException(String message) {
        super(message);
    }

    /**
     * Creates with given cause.
     *
     * @param message Exception message.
     * @param cause Root cause.
     */
    public IntegrityException(String message, Throwable cause) {
        super(message, cause);
    }
}
