package com.vmturbo.cost.component.savings;

import javax.annotation.Nonnull;

/**
 * Exception thrown during savings processing.
 */
public class SavingsException extends Exception {
    /**
     * Creates a new exception.
     *
     * @param message Error message.
     * @param cause Embedded exception.
     */
    public SavingsException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new exception.
     *
     * @param message Error message.
     */
    public SavingsException(@Nonnull String message) {
        super(message);
    }
}
