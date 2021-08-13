package com.vmturbo.plan.orchestrator.plan.export;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;

/**
 * A listener for updates to the status of a {@link PlanDestination}.
 */
public interface PlanExportListener {
    /**
     * Called to indicate that a particular {@link PlanDestination} has a new state.
     * This can be called multiple times for the same {@link PlanDestination}.
     **
     * @param updatedDestination the updated destination object
     * @throws PlanExportListenerException If there is an error processing the state change.
     */
    void onPlanDestinationStateChanged(@Nonnull PlanDestination updatedDestination)
        throws PlanExportListenerException;

    /**
     * Called to indicate that an export to a {@link PlanDestination} is making progress.
     * This can be called multiple times for the same {@link PlanDestination}.
     **
     * @param updatedDestination the updated destination object
     * @throws PlanExportListenerException If there is an error processing the progress.
     */
    void onPlanDestinationProgress(@Nonnull PlanDestination updatedDestination)
        throws PlanExportListenerException;

    /**
     * An exception that indicates an error processing a status update to a
     * {@link PlanDestination}.
     */
    class PlanExportListenerException extends Exception {
        public PlanExportListenerException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }
}
