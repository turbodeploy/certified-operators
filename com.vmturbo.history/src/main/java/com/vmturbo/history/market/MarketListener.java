package com.vmturbo.history.market;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start.SkippedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.stats.PriceIndexWriter;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * Receive and process both projected topologyies and new price index values calculated by
 * Market analysis.
 *
 * Note that this applies to both Live Market topologies and Plan Market topologies.
 *
 * The processing sequence is:
 * <ol>
 *     <li>TopologyProcessor:  topology ->  Market
 *     <li>Market: projectedTopology -> ProjectedTopologyListener.
 *     <li>Market: priceIndexMessage -> PriceIndexListener.
 * </ol>
 * There is no assurance of ordering between the ProjectedTopology and PriceIndex messages.
 */
public class MarketListener implements ProjectedTopologyListener {

    private static Logger logger = LogManager.getLogger(MarketListener.class);

    private final PlanStatsWriter planStatsWriter;
    private final PriceIndexWriter priceIndexWriter;
    private final long realtimeTopologyContextId;
    private final StatsAvailabilityTracker availabilityTracker;
    private final ProjectedStatsStore projectedStatsStore;

    /**
     * Constructs a listener class for the Projected Topologies and Price Index information
     * produced by the Market.
     *
     * @param planStatsWriter the DB access class for the stats from the plan topology
     * @param realtimeTopologyContextId the context ID of a realtime topology.
     * @param statsAvailabilityTracker sends notifications when stats are available.
     */
    public MarketListener(@Nonnull final PlanStatsWriter planStatsWriter,
                          @Nonnull final PriceIndexWriter priceIndexWriter,
                          final long realtimeTopologyContextId,
                          @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker,
                          @Nonnull final ProjectedStatsStore projectedStatsStore) {
        this.planStatsWriter = Objects.requireNonNull(planStatsWriter);
        this.priceIndexWriter = priceIndexWriter;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.availabilityTracker = Objects.requireNonNull(statsAvailabilityTracker);
        this.projectedStatsStore = Objects.requireNonNull(projectedStatsStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                        @Nonnull final TopologyInfo sourceTopologyInfo,
                        @Nonnull final Set<SkippedEntity> skippedEntities,
                        @Nonnull final RemoteIterator<ProjectedTopologyEntity> topologyDTOs) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        final String contextType = topologyContextId == realtimeTopologyContextId ?
            SharedMetrics.LIVE_CONTEXT_TYPE_LABEL : SharedMetrics.PLAN_CONTEXT_TYPE_LABEL;

        SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, contextType)
            .time(() -> {
                if (topologyContextId != realtimeTopologyContextId) {
                    handlePlanProjectedTopology(projectedTopologyId, sourceTopologyInfo, skippedEntities, topologyDTOs);
                } else {
                    handleLiveProjectedTopology(projectedTopologyId, sourceTopologyInfo, skippedEntities, topologyDTOs);
                }
            });
    }

    private void handlePlanProjectedTopology(final long projectedTopologyId,
                     @Nonnull final TopologyInfo sourceTopologyInfo,
                     @Nonnull final Set<SkippedEntity> skippedEntities,
                     @Nonnull final RemoteIterator<ProjectedTopologyEntity> dtosIterator) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        logger.info("Receiving projected plan topology, context: {}, projected id: {}, "
                        + "source id: {}, " + "source topology creation time: {}",
                topologyContextId, projectedTopologyId,
                sourceTopologyInfo.getTopologyId(), sourceTopologyInfo.getCreationTime());

        try {
            int numEntities = planStatsWriter.processProjectedChunks(sourceTopologyInfo, skippedEntities, dtosIterator);
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                    .observe(numEntities);
            availabilityTracker.projectedTopologyAvailable(topologyContextId, TopologyContextType.PLAN);

        } catch (CommunicationException | TimeoutException | InterruptedException
            | VmtDbException e) {
            logger.warn("Error occurred while processing data for projected topology "
                    + "broadcast " + sourceTopologyInfo.getTopologyId(), e);
            throw new RuntimeException("Error occurred while receiving topology broadcast", e);
        }
    }

    private void handleLiveProjectedTopology(final long projectedTopologyId,
                 @Nonnull final TopologyInfo sourceTopologyInfo,
                 @Nonnull final Set<SkippedEntity> skippedEntities,
                 @Nonnull final RemoteIterator<ProjectedTopologyEntity> dtosIterator) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        logger.info("Receiving projected live topology, context: {}, projected id: {}, source id: {}",
                    topologyContextId, projectedTopologyId, sourceTopologyInfo.getTopologyId());
        try {
            final Table<Integer, Long, Double> originalPriceIdxByTypeAndEntity =
                HashBasedTable.create();

            final RemoteIterator<ProjectedTopologyEntity> priceIndexRecordingIterator =
                    new RemoteIterator<ProjectedTopologyEntity>() {
                @Override
                public boolean hasNext() {
                    return dtosIterator.hasNext();
                }

                @Nonnull
                @Override
                public Collection<ProjectedTopologyEntity> nextChunk() throws InterruptedException, TimeoutException, CommunicationException {
                    final Collection<ProjectedTopologyEntity> nextChunk = dtosIterator.nextChunk();
                    for (ProjectedTopologyEntity entity : nextChunk) {
                        if (entity.hasOriginalPriceIndex()) {
                            originalPriceIdxByTypeAndEntity.put(entity.getEntity().getEntityType(),
                                entity.getEntity().getOid(), entity.getOriginalPriceIndex());
                        }
                    }
                    return nextChunk;
                }
            };

            final long numEntities = projectedStatsStore.updateProjectedTopology(priceIndexRecordingIterator);
            logger.info("{} entities updated", numEntities);
            // This needs to happen after updating the projected topology.
            // The projected stats store consumes the iterator, which should fill up the price
            // index map.
            priceIndexWriter.persistPriceIndexInfo(sourceTopologyInfo,
                    originalPriceIdxByTypeAndEntity,
                    skippedEntities);

            availabilityTracker.projectedTopologyAvailable(topologyContextId, TopologyContextType.LIVE);
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                .observe(numEntities);
        } catch (TimeoutException | CommunicationException e) {
            logger.warn("Error occurred while processing data for projected live topology "
                            + "broadcast " + projectedTopologyId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while processing projected live topology " +
                    projectedTopologyId, e);
        }
    }
}