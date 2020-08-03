package com.vmturbo.topology.processor.group.policy.application;

import javax.annotation.Nonnull;

/**
 * An exception that may be thrown when applying a policy.
 */
public class PolicyApplicationException extends Exception {
    /**
     * Create a new instance of the exception.
     *
     * @param cause The exception that caused the policy application failure.
     */
    public PolicyApplicationException(@Nonnull final Exception cause) {
        super(cause);
    }

    /**
     * Createa  new instance of the exception.
     *
     * @param message The error message.
     */
    public PolicyApplicationException(@Nonnull final String message) {
        super(message);
    }
}
