package com.vmturbo.cost.component.savings;

import javax.annotation.Nonnull;

/**
 * Exception thrown during entity savings processing.
 */
public class EntitySavingsException extends Exception {
    /**
     * Creates new instance with specified entity id.
     *
     * @param entityId ID of entity for which exception occurred.
     * @param message Error message.
     * @param cause Embedded exception.
     */
    public EntitySavingsException(long entityId, @Nonnull String message, @Nonnull Throwable cause) {
        super(String.format("[%d]: %s", entityId, message), cause);
    }

    /**
     * Creates new instance with specified entity id.
     *
     * @param entityId ID of entity for which exception occurred.
     * @param message Error message.
     */
    public EntitySavingsException(long entityId, @Nonnull String message) {
        super(String.format("[%d]: %s", entityId, message));
    }

    /**
     * Creates a new exception.
     *
     * @param message Error message.
     * @param cause Embedded exception.
     */
    public EntitySavingsException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new exception.
     *
     * @param message Error message.
     */
    public EntitySavingsException(@Nonnull String message) {
        super(message);
    }
}
