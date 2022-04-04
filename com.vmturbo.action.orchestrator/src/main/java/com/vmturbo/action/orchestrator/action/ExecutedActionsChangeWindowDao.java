package com.vmturbo.action.orchestrator.action;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;

/**
 * DAO for action history.
 */
public interface ExecutedActionsChangeWindowDao {

    /**
     * Persist ExecutedActionsChangeWindow.
     * TODO Reshma will update this method.
     *
     * @param actionId Action ID
     * @param recommendation recommendation
     * @param executionStep execution step
     */
    void persistExecutedActionsChangeWindow(
            long actionId,
            @Nonnull ActionDTO.Action recommendation,
            @Nonnull ExecutionStep executionStep);

    /**
     * Get all ExecutedActionsChangeWindow records by entity OIDs.
     *
     * @param entityOids a list of entity OIDs
     * @return a map that maps entity OID to its corresponding ExecutedActionsChangeWindow
     */
    @Nonnull
    Map<Long, List<ExecutedActionsChangeWindow>> getExecutedActionsChangeWindowMap(List<Long> entityOids);
}
