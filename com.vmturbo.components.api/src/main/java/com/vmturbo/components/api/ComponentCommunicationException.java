package com.vmturbo.components.api;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Base exception class for component APIs and things related to inter-component communication.
 */
public class ComponentCommunicationException extends Exception {

    /**
     * Create a new instance.
     *
     * @param message The error message.
     */
    public ComponentCommunicationException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    /**
     * Create a new instance.
     *
     * @param message The error message.
     * @param cause The cause of the exception.
     */
    public ComponentCommunicationException(@Nonnull final String message,
                              @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}
