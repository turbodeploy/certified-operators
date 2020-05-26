package com.vmturbo.action.orchestrator.exception;

/**
 * This exception is thrown when there are some errors occurred while operating with store (DAO)
 * {@link com.vmturbo.action.orchestrator.action.AcceptedActionsDAO}.
 */
public class AcceptedActionStoreOperationException extends Exception {

    /**
     * Constructs an instance of {@link AcceptedActionStoreOperationException}.
     *
     * @param message Exception message
     */
    public AcceptedActionStoreOperationException(String message) {
        super(message);
    }
}
