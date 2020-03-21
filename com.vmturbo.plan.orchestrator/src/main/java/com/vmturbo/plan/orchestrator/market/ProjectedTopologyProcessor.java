package com.vmturbo.plan.orchestrator.market;

import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * A handler for the projected topology received by the plan orchestrator.
 *
 * <p/>Note: Right now we don't expect two {@link ProjectedTopologyProcessor}s to apply to the
 * same topology, so {@link ProjectedTopologyProcessor#handleProjectedTopology(long, TopologyInfo, RemoteIterator)}
 * accepts the {@link RemoteIterator} directly. If we DO need two processors for the same topology
 * we should add another abstraction layer (like a visitor) to allow the processor to pick what
 * it needs from the incoming topology.
 */
public interface ProjectedTopologyProcessor {

    /**
     * Return whether or not this processor applies to a particular topology.
     * The most common way to decide is by looking at the plan project type.
     *
     * @param sourceTopologyInfo The {@link TopologyInfo} describing the source topology.
     * @return True if this processor applies to the projected topology derived from this source
     * topology. If true, the processor can expect a follow-up call to
     * {@link ProjectedTopologyProcessor#handleProjectedTopology(long, TopologyInfo, RemoteIterator)}.
     */
    boolean appliesTo(@Nonnull TopologyInfo sourceTopologyInfo);

    /**
     * Handle the projected topology. This only gets called if
     * {@link ProjectedTopologyProcessor#appliesTo(TopologyInfo)} returns true for this topology.
     *
     * @param projectedTopologyId The id of the projected topology.
     * @param sourceTopologyInfo {@link TopologyInfo} describing the source topology.
     * @param iterator The {@link RemoteIterator} over the {@link ProjectedTopologyEntity}s in the projected topology.
     * @throws InterruptedException If interrupted while waiting for next chunk of entities.
     * @throws TimeoutException If timed out waiting for next chunk of entities.
     * @throws CommunicationException If there is another communication-related error when waiting for chunks.
     */
    void handleProjectedTopology(long projectedTopologyId,
                                 @Nonnull TopologyInfo sourceTopologyInfo,
                                 @Nonnull RemoteIterator<ProjectedTopologyEntity> iterator)
        throws InterruptedException, TimeoutException, CommunicationException;
}
