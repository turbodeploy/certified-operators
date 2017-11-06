package com.vmturbo.plan.orchestrator.project;

/**
 * Exception to be thrown when PlanProjectInfo is expected to be in a plan project, but it is not.
 */
public class PlanProjectInfoNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public PlanProjectInfoNotFoundException(long planProjectId) {
        super("Plan project with id " + planProjectId + " does not have PlanProjectInfo.");
    }
}
