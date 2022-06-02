package com.vmturbo.action.orchestrator.action;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;
import com.vmturbo.common.protobuf.action.ActionDTO.UpdateActionChangeWindowRequest.ActionLivenessInfo;

/**
 * DAO for executed actions change window.
 */
public interface ExecutedActionsChangeWindowDao {
    /**
     * Save entry on initial successful action execution. This doesn't set the start time, which
     * will get updated later.
     *
     * @param actionId the actionId.
     * @param entityId the entityId.
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    void saveExecutedAction(long actionId, long entityId) throws ActionStoreOperationException;

    /**
     * Updates either the start time, or the end time, along with liveness state for existing
     * action change window entries.
     *
     * @param actionLivenessInfo Set of updates that need to be made.
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    void updateActionLivenessInfo(@Nonnull Set<ActionLivenessInfo> actionLivenessInfo)
        throws ActionStoreOperationException;

    /**
     * Gets a stream of action change windows that match the input liveness states set.
     *
     * @param livenessStates Action change records will be fetched that match these states.
     * @param actionIds Usually empty. If non-empty, then only records matching the specified
     *      action ids will be returned.
     * @param nextCursor Cursor to use for next page of queries. Start with 0 first time.
     * @param consumer The one who consumes the stream of live action change window entries.
     *      Note that the returned ExecutedActionsChangeWindow entries will NOT have the ActionSpec
     *      field set, as they are not yet available.
     * @return Optional next cursor to use for next query, if using paged results.
     * @throws ActionStoreOperationException Thrown on record store error.
     */
    Optional<Long> getActionsByLivenessState(@Nonnull Set<LivenessState> livenessStates,
            @Nonnull Set<Long> actionIds,
            long nextCursor,
            @Nonnull Consumer<ExecutedActionsChangeWindow> consumer)
            throws ActionStoreOperationException;

    /**
     * Similar to the other one, but with empty action id, so will get records for all actions.
     * @see #getActionsByLivenessState(Set, Set, long, Consumer)
     */
    default Optional<Long> getActionsByLivenessState(@Nonnull final Set<LivenessState> livenessStates,
            long nextCursor,
            @Nonnull Consumer<ExecutedActionsChangeWindow> consumer)
            throws ActionStoreOperationException {
        return getActionsByLivenessState(livenessStates, Collections.emptySet(), nextCursor, consumer);
    }

    /**
     * Get all ExecutedActionsChangeWindow records by entity OIDs.
     *
     * @param entityOids a list of entity OIDs
     * @return a map that maps entity OID to its corresponding ExecutedActionsChangeWindow
     */
    @Nonnull
    Map<Long, List<ExecutedActionsChangeWindow>> getActionsByEntityOid(List<Long> entityOids);
}
