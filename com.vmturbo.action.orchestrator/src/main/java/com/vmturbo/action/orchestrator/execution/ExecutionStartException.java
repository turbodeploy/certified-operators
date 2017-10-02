package com.vmturbo.action.orchestrator.execution;

import javax.annotation.Nonnull;

/**
 * Indicates an error during the start of an {@link com.vmturbo.action.orchestrator.action.ExecutableStep}.
 */
public class ExecutionStartException extends Exception {

    public ExecutionStartException(@Nonnull final String message) {
        super(message);
    }

    public ExecutionStartException(@Nonnull final String message, @Nonnull final Throwable cause) {
        super(message, cause);
    }
}
