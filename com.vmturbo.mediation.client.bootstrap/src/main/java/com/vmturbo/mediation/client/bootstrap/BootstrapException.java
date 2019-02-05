package com.vmturbo.mediation.client.bootstrap;

/**
 * Exception to be throws when bootstrapping failed.
 */
public class BootstrapException extends Exception {

    /**
     * Constructs exception with a message and root cause.
     *
     * @param message message
     * @param cause root cause
     */
    public BootstrapException(String message, Throwable cause) {
        super(message, cause);
    }
}
