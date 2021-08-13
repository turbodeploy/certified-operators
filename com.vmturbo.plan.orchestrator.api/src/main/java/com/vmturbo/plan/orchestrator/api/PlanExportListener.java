package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;

/**
 * A listener for updates to the status of a {@link PlanDestination}.
 */
public interface PlanExportListener {
    /**
     * Called to indicate that a particular {@link PlanDestination} has a new state.
     * This can be called multiple times for the same {@link PlanDestination}.
     *
     * @param updatedDestination the updated plan destination
     */
    void onPlanDestinationStateChanged(@Nonnull PlanDestination updatedDestination);

    /**
     * Called to indicate that an export to a {@link PlanDestination} is making progress.
     * This can be called multiple times for the same {@link PlanDestination}.
     **
     * @param updatedDestination the updated plan destination
     */
    void onPlanDestinationProgress(@Nonnull PlanDestination updatedDestination);
}
