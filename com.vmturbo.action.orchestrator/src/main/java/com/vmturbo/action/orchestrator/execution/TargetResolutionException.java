package com.vmturbo.action.orchestrator.execution;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ExecutableStep;

/**
 * No target found that can execute an {@link ExecutableStep}.
 */
public class TargetResolutionException extends Exception {

    public TargetResolutionException(@Nonnull final String message) {
        super(message);
    }

    public TargetResolutionException(@Nonnull final String message, @Nonnull final Throwable cause) {
        super(message, cause);
    }
}
