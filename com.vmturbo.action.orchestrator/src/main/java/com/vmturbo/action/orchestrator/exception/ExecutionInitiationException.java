package com.vmturbo.action.orchestrator.exception;

import io.grpc.Status;
import io.grpc.StatusException;

/**
 * An exception which indicates failure in initiating execution of an action.
 */
public class ExecutionInitiationException extends Exception {
    final Status.Code code;

    /**
     * Creates a new instance of this exception.
     * @param description description of the exception.
     * @param code the status code.
     */
    public ExecutionInitiationException(String description, Status.Code code) {
        super(description);
        this.code = code;
    }

    /**
     * Creates a new instance of this exception.
     * @param description description of the exception.
     * @param e exception causing this exception.
     * @param code the status code.
     */
    public ExecutionInitiationException(String description, Throwable e, Status.Code code) {
        super(description, e);
        this.code = code;
    }

    /**
     * Converts the exception to status exception.
     *
     * @return the status exception.
     */
    public StatusException toStatus() {
        return Status.fromCode(code).withDescription(getMessage()).asException();
    }
}
