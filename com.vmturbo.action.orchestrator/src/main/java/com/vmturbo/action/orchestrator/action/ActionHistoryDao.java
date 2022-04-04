package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.db.tables.pojos.ActionHistory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;

/**
 * DAO for action history.
 */
public interface ActionHistoryDao {

    /**
     * Persist a action history, based on Action {@link Action}. It's intended to persist executed
     * action which has either SUCCEEDED or FAILED state. And it should be added and not updated.
     *
     * @param actionId The id of the action.
     * @param recommendation The {@link ActionDTO.Action} describing the action.
     * @param realtimeTopologyContextId The ID of the realtime topology context.
     * @param recommendationTime The time the action was originally recommended.
     * @param decision Descriptor of the action decision.
     * @param executionStep Descriptor for the current execution state.
     * @param currentState Descriptor for the current state.
     * @param actionDetailData Detail about the action (used to help reconstruct descriptions and
     *                         other topology-related data).
     * @param associatedAccountId The ID of the associated business account, if any.
     * @param associatedResourceGroupId The ID of the associated resource group, if any.
     * @param recommendationOid OID of market recommendation
     * @return action history, if created
     */
    @Nonnull
    ActionHistory persistActionHistory(
            long actionId,
            @Nonnull ActionDTO.Action recommendation,
            long realtimeTopologyContextId,
            @Nonnull LocalDateTime recommendationTime,
            @Nullable ActionDecision decision,
            @Nullable ExecutionStep executionStep,
            int currentState,
            @Nullable byte[] actionDetailData,
            @Nullable Long associatedAccountId,
            @Nullable Long associatedResourceGroupId,
            long recommendationOid);

    /**
     * Returns all the existing action history filtered by action query filter parameters, like 'startDate' and
     * 'endDate', and scope filters.
     *
     * @param actionQueryFilter The filter parameters by which to filter the actions
     * @return List of {@link Action} matching the filters in the action query filter.
     */
    @Nonnull
    List<ActionView> getActionHistoryByFilter(@Nonnull ActionQueryFilter actionQueryFilter);

    /**
     * Get historical actions by a list of action IDs.
     *
     * @param actionIds list of action IDs
     * @return List of {@link Action}
     */
    @Nonnull
    List<ActionView> getActionHistoryByIds(@Nonnull List<Long> actionIds);
}
