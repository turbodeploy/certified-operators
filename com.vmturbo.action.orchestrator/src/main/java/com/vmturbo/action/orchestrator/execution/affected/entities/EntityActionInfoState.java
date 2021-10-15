package com.vmturbo.action.orchestrator.execution.affected.entities;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * Limited set of states that the entity action info controllable flags use.
 */
public enum EntityActionInfoState {

    /**
     * The action is in progress (QUEUE, PRE, REPLACE, POST).
     */
    IN_PROGRESS,
    /**
     * The action succeeded.
     */
    SUCCEEDED,
    /**
     * The action failed.
     */
    FAILED;

    /**
     * Converts ActionOrchestrator state to EntityActionInfoState.
     *
     * @param state the state in ActionOrchestration.
     * @return Optional.empty() if EntityActionInfo does not care about that state.
     *                          Otherwise returns the simplified EntityActionInfo state.
     */
    public static Optional<EntityActionInfoState> getState(@Nonnull ActionState state) {
        // treat all intermediate states as IN_PROGRESS for affected entities logic
        switch (state) {
            case READY:
            case CLEARED:
            case ACCEPTED:
            case REJECTED:
                return Optional.empty();
            case QUEUED:
            case PRE_IN_PROGRESS:
            case IN_PROGRESS:
            case POST_IN_PROGRESS:
            case FAILING:
                return Optional.of(EntityActionInfoState.IN_PROGRESS);
            case SUCCEEDED:
                return Optional.of(EntityActionInfoState.SUCCEEDED);
            case FAILED:
                return Optional.of(EntityActionInfoState.FAILED);
            default:
                throw new IllegalStateException(state + " is not supported by EntityActionInfoState");
        }
    }
}
