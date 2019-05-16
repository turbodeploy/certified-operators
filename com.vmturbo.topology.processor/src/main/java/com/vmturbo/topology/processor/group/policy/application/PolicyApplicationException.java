package com.vmturbo.topology.processor.group.policy.application;

import javax.annotation.Nonnull;

/**
 * An exception that may be thrown when applying a policy.
 */
public class PolicyApplicationException extends Exception {
    public PolicyApplicationException(@Nonnull final RuntimeException e) {
        super(e);
    }

    public PolicyApplicationException(@Nonnull final String message) {
        super(message);
    }
}
