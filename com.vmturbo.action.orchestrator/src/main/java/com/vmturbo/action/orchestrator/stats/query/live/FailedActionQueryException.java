package com.vmturbo.action.orchestrator.stats.query.live;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusException;

/**
 * Generic exception thrown when the {@link CurrentActionStatReader} fails to execute a query.
 */
public class FailedActionQueryException extends Exception {

    private final Status status;

    FailedActionQueryException(@Nonnull final Status status,
                               @Nonnull final String message) {
        super(message);
        this.status = status;
    }

    FailedActionQueryException(@Nonnull final Status status,
                               @Nonnull final String message,
                               @Nonnull final Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    @Nonnull
    public StatusException asGrpcException() {
        return status.withDescription(getMessage())
            .withCause(getCause())
            .asException();
    }
}
