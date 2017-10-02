package com.vmturbo.action.orchestrator.diagnostics;

import javax.annotation.Nonnull;

/**
 * Indicates that the input zip to restore the
 * internal state of the action orchestrator is erroneous
 * in some way.
 */
public class InvalidRestoreInputException extends Exception {

    public InvalidRestoreInputException(@Nonnull final String message) {
        super(message);
    }

    public InvalidRestoreInputException(@Nonnull final String message, @Nonnull final Exception cause) {
        super(message, cause);
    }
}
