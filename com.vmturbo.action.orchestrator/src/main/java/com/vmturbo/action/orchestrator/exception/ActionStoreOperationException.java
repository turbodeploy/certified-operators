package com.vmturbo.action.orchestrator.exception;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;

/**
 * This exception is thrown when there are some errors occurred while operating with store (DAO)
 * {@link AcceptedActionsDAO}.
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
}
