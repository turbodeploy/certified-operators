package com.vmturbo.topology.processor.group.policy.application;

import javax.annotation.Nonnull;

/**
 * An exception that may be thrown when merge policy type is unknown.
 */
public class InvalidMergePolicyTypeException extends RuntimeException {
    public InvalidMergePolicyTypeException(@Nonnull final RuntimeException e) {
        super(e);
    }

    public InvalidMergePolicyTypeException(@Nonnull final String message) {
        super(message);
    }
}
