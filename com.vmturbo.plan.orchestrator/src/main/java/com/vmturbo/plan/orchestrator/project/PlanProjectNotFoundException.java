package com.vmturbo.plan.orchestrator.project;

/**
 * Exception to be thrown when plan project is expected to be in a plan database, but it is not.
 */
public class PlanProjectNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public PlanProjectNotFoundException(long planProjectId) {
        super("Plan project with id " + planProjectId + " does not exist in the plan database.");
    }
}
