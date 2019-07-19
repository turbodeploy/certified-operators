package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.db.tables.pojos.ActionHistory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;

/**
 * DAO for action history.
 */
public interface ActionHistoryDao {

    /**
     * Persist a action history, based on Action {@link Action}. It's intended to persist executed
     * action which has either SUCCEEDED or FAILED state. And it should be added and not updated.
     *
     * @return action history, if created
     */
    @Nonnull
    ActionHistory persistActionHistory(
            final long actionId,
            @Nonnull final ActionDTO.Action recommendation,
            final long realtimeTopologyContextId,
            @Nonnull final LocalDateTime recommedationTime,
            @Nullable final ActionDecision decision,
            @Nullable final ExecutionStep executionStep,
            final int currentState);

    /**
     * Returns all the existing action history between 'startDate' and 'endDate'.
     *
     * @param startDate the start date
     * @param endDate   the end date
     * @return List of {@link Action} within the startDate and endDate.
     */
    @Nonnull
    List<ActionView> getActionHistoryByDate(@Nonnull final LocalDateTime startDate,
                                            @Nonnull final LocalDateTime endDate);
}
