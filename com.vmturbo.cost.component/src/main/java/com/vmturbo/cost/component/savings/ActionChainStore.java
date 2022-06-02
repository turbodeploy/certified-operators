package com.vmturbo.cost.component.savings;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;

/**
 * Interface to get ActionChainRecords (or equivalent protobuf) from AO via gRPC.
 */
public interface ActionChainStore {
    /**
     * Gets a map of entityId to a set of actions for that entity, sorted by completion time asc.
     *
     * @param entityIds Chunk of entity ids for which action chain records need to be fetched.
     * @return For each entityId, the sorted action chain.
     */
    @Nonnull
    Map<Long, NavigableSet<ExecutedActionsChangeWindow>> getActionChains(@Nonnull Set<Long> entityIds);
}
