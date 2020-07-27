package com.vmturbo.cost.component.reserved.instance;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * Listener that receives the projected topology from the market
 * and forwards them to the classes in the cost component that store them and make them
 * available for queries.
 */
public class CostComponentProjectedEntityTopologyListener implements
        ProjectedTopologyListener {

    private static final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;
    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;
    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final Object topologyInfoLock = new Object();
    @GuardedBy("topologyInfoLock")
    private long latestKnownProjectedTopologyId = -1;

    CostComponentProjectedEntityTopologyListener(final long realtimeTopopologyContextId,
             @Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
             @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory) {
        this.realtimeTopologyContextId = realtimeTopopologyContextId;
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
    }

    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                                            @Nonnull final TopologyInfo originalTopologyInfo,
                                            @Nonnull final RemoteIterator<ProjectedTopologyEntity>
                                                    projectedTopo,
                                            @Nonnull final SpanContext tracingContext) {
        logger.info("Received projected entities for topologyId {}", projectedTopologyId);
        try {
            onProjectedTopologyReceivedInternal(projectedTopologyId, originalTopologyInfo,
                    projectedTopo, tracingContext);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Failed to process the projected topology that was received " +
                    projectedTopologyId, e);
        }
    }

    private void onProjectedTopologyReceivedInternal(long projectedTopologyId,
                                                    TopologyInfo originalTopologyInfo,
                                                    @Nonnull final RemoteIterator<ProjectedTopologyEntity>
                                                            projectedTopo,
                                                    @Nonnull final SpanContext tracingContext)
            throws CommunicationException, InterruptedException {
        updateLatestKnownProjectedTopologyId(projectedTopologyId);

        final long topologyContextId = originalTopologyInfo.getTopologyContextId();

        if (shouldSkipTopology(originalTopologyInfo, projectedTopologyId)) {
            try {
                // drain the iterator
                while (projectedTopo.hasNext()) {
                    projectedTopo.nextChunk();
                }
            } catch (TimeoutException e) {
                logger.warn("TimeoutException while skipping projected topology {}",
                        projectedTopologyId);
            }
            return;
        }

        try (TracingScope scope = Tracing.trace("cost_handle_projected_topology", tracingContext)) {
            if (topologyContextId != realtimeTopologyContextId) {
                handlePlanProjectedTopology(projectedTopologyId, originalTopologyInfo, projectedTopo);
            } else {
                handleLiveProjectedTopology(projectedTopologyId, originalTopologyInfo, projectedTopo);
            }
        }
    }

    private void handlePlanProjectedTopology(final long projectedTopologyId,
                                             @Nonnull final TopologyInfo sourceTopologyInfo,
                                             @Nonnull final RemoteIterator<ProjectedTopologyEntity>
                                                     projectedTopo) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        logger.info("Received projected plan topology, context: {}, projected id: {}, "
                        + "source id: {}, " + "source topology creation time: {}",
                topologyContextId, projectedTopologyId,
                sourceTopologyInfo.getTopologyId(), sourceTopologyInfo.getCreationTime());
        try {
            // drain the iterator
            while (projectedTopo.hasNext()) {
                projectedTopo.nextChunk();
            }
        } catch (TimeoutException | CommunicationException e) {
            logger.warn("Error occurred while processing data for projected topology "
                    + "broadcast " + projectedTopologyId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while processing projected live topology " +
                    projectedTopologyId, e);
        }
    }

    private void handleLiveProjectedTopology(final long projectedTopologyId,
                                             @Nonnull final TopologyInfo sourceTopologyInfo,
                                             @Nonnull final RemoteIterator<ProjectedTopologyEntity> dtosIterator) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        logger.info("Received projected live topology, context: {}, projected id: {}, source id: {}",
                topologyContextId, projectedTopologyId, sourceTopologyInfo.getTopologyId());

        try {
            Set<TopologyEntityDTO> cloudEntities = new HashSet<>();
            while (dtosIterator.hasNext()) {
                dtosIterator.nextChunk().stream()
                        .filter(this::isProjectedCloudEntity)
                        .forEach(projectedEntity -> cloudEntities.add(projectedEntity.getEntity()));
            }

            // if no Cloud entity, skip further processing
            if (cloudEntities.size() > 0) {
                final CloudTopology<TopologyEntityDTO> cloudTopology =
                        cloudTopologyFactory.newCloudTopology(cloudEntities.stream());
                // Store consumption demand in db
                // Note that we are passing in sourceTopologyInfo.  This is used to retrieve
                // previously written context for source topology (persisted with
                // isSourceTopology = false via LiveTopologyEntitiesListener.
                computeTierDemandStatsWriter.calculateAndStoreRIDemandStats(sourceTopologyInfo,
                        cloudTopology, true);
            } else {
                logger.info("live projected topology with topologyId: {}  doesn't " +
                        "have Cloud entity, skip processing", sourceTopologyInfo.getTopologyId());
            }
        } catch (TimeoutException | CommunicationException e) {
            logger.warn("Error occurred while processing data for projected live " +
                    "topology broadcast " + projectedTopologyId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while processing projected live topology " +
                    projectedTopologyId, e);
        }
    }

    private boolean isProjectedCloudEntity(@Nonnull final ProjectedTopologyEntity projEntity) {
        return projEntity.getEntity().getEnvironmentType() == EnvironmentType.CLOUD;
    }

    private boolean shouldSkipTopology(TopologyInfo originalTopologyInfo,
                                          long projectedTopologyId) {
        // Don't skip plan projected topology
        if (originalTopologyInfo.getTopologyType() == TopologyType.REALTIME) {
            synchronized (topologyInfoLock) {
                // We will skip this projected topology if the corresponding source
                // topology was not persisted
                if (!computeTierDemandStatsWriter.hasExistingStats(originalTopologyInfo)) {
                    // skip this projected topology because demand from source topology was not
                    // persisted
                    logger.info("Skipping projected topology id {} because demand from " +
                                    "source topology {} was not persisted",
                            projectedTopologyId, originalTopologyInfo.getTopologyId());
                    return true;
                }
            }
        }
        return false;
    }

    private void updateLatestKnownProjectedTopologyId(final long id) {
        synchronized (topologyInfoLock) {
            latestKnownProjectedTopologyId = Math.max(id, latestKnownProjectedTopologyId);
        }
    }
}
