package com.vmturbo.action.orchestrator.exception;

/**
 * This exception is thrown when there are some errors occurred while operating with store (DAO).
 */
public class ActionStoreOperationException extends Exception {

    /**
     * Constructs an instance of {@link ActionStoreOperationException}.
     *
     * @param message Exception message
     */
    public ActionStoreOperationException(String message) {
        super(message);
    }

    /**
     * Constructs an instance of {@link ActionStoreOperationException}.
     *
     * @param message Exception message
     * @param exception The caused by exception.
     */
    public ActionStoreOperationException(String message, Exception exception) {
        super(message, exception);
    }
}
