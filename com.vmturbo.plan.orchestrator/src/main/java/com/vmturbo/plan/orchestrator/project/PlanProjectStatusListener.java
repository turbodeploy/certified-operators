package com.vmturbo.plan.orchestrator.project;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;

/**
 * Interface for listening to plan project related changes, invoked when any underlying plans
 * in the project change status.
 */
public interface PlanProjectStatusListener {

    /**
     * Called when status of plan project is changed.
     *
     * @param planProject Plan project for which status is changed.
     * @throws PlanProjectStatusListenerException Thrown on status change issues.
     */
    void onPlanStatusChanged(@Nonnull PlanProject planProject)
            throws PlanProjectStatusListenerException;

    /**
     * Exception to notify plan project status notification related issues.
     */
    class PlanProjectStatusListenerException extends Exception {
        /**
         * Creates a new instance.
         *
         * @param cause Base exception to use during creation.
         */
        public PlanProjectStatusListenerException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }
}