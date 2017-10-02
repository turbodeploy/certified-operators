package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * An interface for factories that create actions.
 */
public interface IActionFactory {
    /**
     * Create a new Action instance. The created action will receive a recommendation time of
     * the current time.
     *
     * @param recommendation The market recommendation for the action in the environment.
     * @param actionPlanId The ID of the ActionPlan the recommendation was a part of.
     * @return A new {@link Action} instance.
     */
    @Nonnull
    Action newAction(@Nonnull final ActionDTO.Action recommendation,
                     final long actionPlanId);

    /**
     * Create a new Action instance.
     *
     * @param recommendation The market recommendation for the action in the environment.
     * @param recommendationTime The time at which the action was recommended.
     * @param actionPlanId The ID of the ActionPlan the recommendation was a part of.
     * @return A new {@link Action} instance.
     */
    @Nonnull
    Action newAction(@Nonnull final ActionDTO.Action recommendation,
                     @Nonnull final LocalDateTime recommendationTime,
                     final long actionPlanId);
}
