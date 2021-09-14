package com.vmturbo.topology.processor.workflow;

/**
 * Class containing the details about the workflow execution result.
 */
public class WorkflowExecutionResult {

    final Boolean succeeded;
    final String executionDetails;

    /**
     * Class constructor.
     *
     * @param succeeded Flag set to true if the workflow execution has succeeded, false otherwise.
     * @param executionDetails The details about the workflow execution.
     */
    public WorkflowExecutionResult(Boolean succeeded, String executionDetails) {
        this.succeeded = succeeded;
        this.executionDetails = executionDetails;
    }

    /**
     * Return the current value of the <code>succeeded</code> instance.
     *
     * @return succeeded The value of the succeeded flag.
     */
    public Boolean getSucceeded() {
        return this.succeeded;
    }

    /**
     * Return the current value of the <code>executionDetails</code> instance.
     *
     * @return executionDetails The string containing the workflow execution details.
     */
    public String getExecutionDetails() {
        return this.executionDetails;
    }

}
