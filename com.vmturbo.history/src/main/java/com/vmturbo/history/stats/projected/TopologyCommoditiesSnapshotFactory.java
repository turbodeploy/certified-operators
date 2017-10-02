package com.vmturbo.history.stats.projected;

import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * A factory for {@link TopologyCommoditiesSnapshot}, used for dependency
 * injection for unit tests. We don't really need a factory otherwise, since
 * all of these classes are private to the {@link ProjectedStatsStore} implementation.
 */
interface TopologyCommoditiesSnapshotFactory {

    @Nonnull
    TopologyCommoditiesSnapshot createSnapshot(final @Nonnull RemoteIterator<TopologyEntityDTO> entities)
        throws InterruptedException, TimeoutException, CommunicationException;

}
