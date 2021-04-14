package com.vmturbo.topology.processor.entity;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.communication.CommunicationException;

/**
 * An interface to be implemented by classes that wish to receive incremental discovery updates from the {@link EntityStore}.
 */
public interface EntitiesWithNewStateListener {

    /**
     * A callback to be called when there's an incremental discovery update from {@link EntityStore}.
     *
     * @param entitiesWithNewState entities with new state
     * @throws CommunicationException if an error occurs in the communication
     * @throws InterruptedException if the operation is interrupted
     */
    void onEntitiesWithNewState(EntitiesWithNewState entitiesWithNewState) throws CommunicationException, InterruptedException;
}
