package com.vmturbo.components.api.client;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.ComponentCommunicationException;

/**
 * Exception thrown by an {@link ComponentApiClient}
 * in case of a processing error.
 */
public class ApiClientException extends ComponentCommunicationException {

    public ApiClientException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    public ApiClientException(@Nonnull final String message,
                              @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}
