package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;

/**
 * Listener for {@link EntitiesWithNewState} events.
 */
public interface EntitiesWithNewStateListener {

    /**
     * Called when a new {@link EntitiesWithNewState} message is received.
     *
     * @param entitiesWithNewState The message that contains the new host state.
     */
    void onEntitiesWithNewState(@Nonnull EntitiesWithNewState entitiesWithNewState);

}
