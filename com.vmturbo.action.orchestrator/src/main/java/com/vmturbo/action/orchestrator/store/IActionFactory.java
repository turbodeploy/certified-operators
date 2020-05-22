package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * @param recommendationOid OID of market recommendation
     * @return A new {@link Action} instance.
     */
    @Nonnull
    Action newAction(@Nonnull ActionDTO.Action recommendation,
                     long actionPlanId, long recommendationOid);

    /**
     * Create a new Action instance. This method is used when creating {@link Action}s to represent
     * actions saved as part of a plan, and therefore has more parameters.
     *
     * @param recommendation The market recommendation for the action in the environment.
     * @param recommendationTime The time at which the action was recommended.
     * @param actionPlanId The ID of the ActionPlan the recommendation was a part of.
     * @param description The description of the recommendation.
     * @param associatedAccountId The business account associated with this action. Null if none.
     * @param associatedResourceGroupId The resource group associated with this action. Null if
     * none.
     * @return A new {@link Action} instance.
     */
    @Nonnull
    Action newPlanAction(@Nonnull ActionDTO.Action recommendation,
                         @Nonnull LocalDateTime recommendationTime,
                         long actionPlanId,
                         @Nullable String description,
                         @Nullable Long associatedAccountId,
                         @Nullable Long associatedResourceGroupId);
}
