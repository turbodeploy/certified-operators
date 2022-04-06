package com.vmturbo.action.orchestrator.action;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;

/**
 * DAO for executed actions change window.
 */
public interface ExecutedActionsChangeWindowDao {

    /**
     * Persist an executed action change window record, based on Action {@link Action} and execution details.
     *
     * <p>It's intended to persist action change window details of Succeeded actions. It should be added to,
     * and not updated.
     *
     * @param actionId the actionId.
     * @param entityId the entityId.
     * @param completionTime the change window's start time is the time that the action completed successfully.
     * @return ExecutedActionsChangeWindow, if created.
     */
     void persistExecutedActionsChangeWindow(long actionId, long entityId, long completionTime);

    /**
     * Get all ExecutedActionsChangeWindow records by entity OIDs.
     *
     * @param entityOids a list of entity OIDs
     * @return a map that maps entity OID to its corresponding ExecutedActionsChangeWindow
     */
    @Nonnull
    Map<Long, List<ExecutedActionsChangeWindow>> getExecutedActionsChangeWindowMap(List<Long> entityOids);
}
