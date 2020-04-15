package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.PlanDeleted;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.StatusUpdate;

/**
 * Listener for plan-related events.
 */
public interface PlanListener {
    /**
     * Called to indicate that a plan has a new status - usually called when a major part of the
     * plan lifecycle got completed (e.g. market analysis is done).
     *
     * <p/>There is no formal state machine for plans, but generally they proceed in the order they
     * are defined in the {@link PlanStatus} enum, terminating in SUCCEDED, FAILED, or STOPPED
     * (if interrupted by the user).
     *
     * @param planStatusUpdate The message describing the update.
     */
    void onPlanStatusChanged(@Nonnull StatusUpdate planStatusUpdate);

    /**
     * Called to indicate that a plan got deleted and no longer exists in the plan orchestrator.
     * The listener should stop any process related to the plan, and delete any data related
     * to the plan.
     *
     * @param planDeleted The message describing the delete.
     */
    void onPlanDeleted(@Nonnull PlanDeleted planDeleted);
}
