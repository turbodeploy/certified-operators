package com.vmturbo.action.orchestrator.workflow.store;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO;

/**
 * Checked Exception thrown if there's an error persisting a workflow.
 **/
public class PersistWorkflowException extends Exception {

    /**
     * Error while persisting {@link WorkflowDTO.WorkflowInfo}s.
     *
     * @param message a message describing what was going on when the error occurred
     */
    public PersistWorkflowException(String message) {
        super(message);
    }

    /**
     * Error while persisting {@link WorkflowDTO.WorkflowInfo}s, including the original Throwable.
     * @param message a message describing what was going on when the error occurred
     * @param cause a Throwable that precipitated this error handling
     */
    public PersistWorkflowException(String message, Throwable cause) {
        super(message, cause);
    }
}
