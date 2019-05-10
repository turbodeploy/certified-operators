package com.vmturbo.topology.processor.entity;

/**
 * Exception thrown when entity can not be found in EntityStore.
 */
public class EntityNotFoundException extends RuntimeException {

    /**
     * Constructs a new EntityNotFoundException with the specified detail message.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public EntityNotFoundException(final String message) {
        super(message);
    }
}
