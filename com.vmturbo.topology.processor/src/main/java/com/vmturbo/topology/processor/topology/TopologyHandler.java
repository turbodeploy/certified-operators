package com.vmturbo.topology.processor.topology;

import java.time.Clock;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;

/**
 * Stores topology snapshots per-target and broadcasts the results to listening components.
 */
@ThreadSafe
public class TopologyHandler {
    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final IdentityProvider identityProvider;

    private final Clock clock;

    private final EntityStore entityStore;

    private TopologyPipelineFactory topologyPipelineFactory;

    public TopologyHandler(final long realtimeTopologyContextId,
                           @Nonnull final TopologyPipelineFactory topologyPipelineFactory,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final Clock clock) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.topologyPipelineFactory = Objects.requireNonNull(topologyPipelineFactory);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.clock = clock;
    }

    /**
     * Broadcast the current topology to other services.
     *
     * @return The count of the total number of entities broadcast.
     * @throws TopologyPipelineException If there is an error broadcasting the topology.
     * @throws InterruptedException If the broadcast is interrupted.
     */
    public synchronized TopologyBroadcastInfo broadcastLatestTopology()
            throws TopologyPipelineException, InterruptedException {

        final TopologyInfo tinfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyId(identityProvider.generateTopologyId())
                .setTopologyContextId(realtimeTopologyContextId)
                .setCreationTime(clock.millis())
                .build();

        return topologyPipelineFactory.liveTopology(tinfo, Collections.emptyList()).run(entityStore);
    }

}
