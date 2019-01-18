package com.vmturbo.action.orchestrator.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;

/**
 * Clients should implement the {@link ActionsListener} to
 * process {@link ActionPlan}s received from the action orchestrator.
 *
 * <p>Listeners need to be registered with the {@link ActionOrchestrator}.
 */
public interface ActionsListener {

    /**
     * Callback receiving the actions the market computed.
     *
     * This method is now deprecated, since the ActionPlan can get pretty large and the bulk of it
     * isn't needed in the listeners. We can safely remove this when we don't feel we need to support any
     * customers on 7.10 or earlier.
     *
     * @param actionPlan The actions recommended by the market.
     */
    @Deprecated
    default void onActionsReceived(@Nonnull final ActionPlan actionPlan) {}

    /**
     * Callback when the actions stored in the ActionOrchestrator have been updated. Replaces the
     * "onActionsReceived" event.
     */
    default void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {}

    /**
     * Callback receiving a progress update for an action.
     *
     * @param actionProgress The description of the progress update.
     */
    default void onActionProgress(@Nonnull final ActionProgress actionProgress) {}

    /**
     * Callback receiving a success update for an action.
     *
     * @param actionSuccess The description of the success update.
     */
    default void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {}

    /**
     * Callback receiving a failure update for an action.
     *
     * @param actionFailure The description of the success update.
     */
    default void onActionFailure(@Nonnull final ActionFailure actionFailure) {}
}
