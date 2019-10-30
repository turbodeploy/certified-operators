package com.vmturbo.group.service;

import javax.annotation.Nonnull;

import io.grpc.Status;

/**
 * Exception indicating that some error occurred while operating with store (DAO) object.
 */
public class StoreOperationException extends Exception {
    private final Status status;

    /**
     * Constructs exception with the specified response status and message.
     *
     * @param status status
     * @param message message
     */
    public StoreOperationException(@Nonnull Status status, String message) {
        super(message);
        this.status = status;
    }

    /**
     * Constructs exception with the specified response status, message and root cause.
     *
     * @param status status
     * @param message message
     * @param cause root cause
     */
    public StoreOperationException(@Nonnull Status status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    /**
     * Returns gRPC response status to be associated with this exception. If no status is specified,
     * Status#INTERNAL is used.
     *
     * @return status
     */
    @Nonnull
    public Status getStatus() {
        return status != null ? status : Status.INTERNAL;
    }
}
