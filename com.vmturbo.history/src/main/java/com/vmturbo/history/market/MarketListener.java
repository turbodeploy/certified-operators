package com.vmturbo.history.market;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.stats.LiveStatsWriter;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.stats.PriceIndexWriter;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.topology.TopologySnapshotRegistry;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.priceindex.api.PriceIndexListener;
import com.vmturbo.reports.db.VmtDbException;

/**
 * Receive and process new price index values calculated by Market analysis.
 *
 * The processing sequence is:
 * <ol>
 *     <li>TopologyProcessor:  topology ->  Market
 *     <li>Market: priceIndexMessage -> MarketListener.
 * </ol>
 */
public class MarketListener implements ProjectedTopologyListener, PriceIndexListener {

    private static Logger logger = LogManager.getLogger(MarketListener.class);

    private final LiveStatsWriter liveStatsWriter;
    private final PlanStatsWriter planStatsWriter;
    private final PriceIndexWriter priceIndexWriter;
    private final long realtimeTopologyContextId;
    private final StatsAvailabilityTracker availabilityTracker;
    private final ProjectedStatsStore projectedStatsStore;

    /**
     * Constructs a listener class for the Price Index information produced by the Market.
     * Note that the PriceIndex API is currently websocket based.
     *
     * @param liveStatsWriter the DB access class for the stats from the Live topology
     * @param planStatsWriter the DB access class for the stats from the plan topology
     * @param snapshotRegistry a {@link TopologySnapshotRegistry} for where a Topology is
     *                         cached so that when the corresponding priceIndex message
     *                         arrives the processing has access to the original topology.
     * @param realtimeTopologyContextId the context ID of a realtime topology.
     * @param statsAvailabilityTracker sends notifications when stats are available.
     */
    public MarketListener(@Nonnull final LiveStatsWriter liveStatsWriter,
                          @Nonnull final PlanStatsWriter planStatsWriter,
                          @Nonnull final PriceIndexWriter priceIndexWriter,
                          @Nonnull final TopologySnapshotRegistry snapshotRegistry,
                          final long realtimeTopologyContextId,
                          @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker,
                          @Nonnull final ProjectedStatsStore projectedStatsStore) {
        this.liveStatsWriter = Objects.requireNonNull(liveStatsWriter);
        this.planStatsWriter = Objects.requireNonNull(planStatsWriter);
        this.priceIndexWriter = priceIndexWriter;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.availabilityTracker = Objects.requireNonNull(statsAvailabilityTracker);
        this.projectedStatsStore = Objects.requireNonNull(projectedStatsStore);
    }

    /**
     * When we receive a projected topology, persist the stats to the mkt_snapshots_stats table.
     *
     * @param projectedTopologyId id of the projected topology
     * @param sourceTopologyInfo contains basic information of the source topology
     * @param topologyDTOs contains the {@link TopologyEntityDTO}s after the plan has completed.
     */
    @Override
    public void onProjectedTopologyReceived(long projectedTopologyId,
                                            TopologyInfo sourceTopologyInfo,
                                            @Nonnull RemoteIterator<TopologyDTO.TopologyEntityDTO> topologyDTOs) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        final String contextType = topologyContextId == realtimeTopologyContextId ?
            SharedMetrics.LIVE_CONTEXT_TYPE_LABEL : SharedMetrics.PLAN_CONTEXT_TYPE_LABEL;

        SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, contextType)
            .time(() -> {
                if (topologyContextId != realtimeTopologyContextId) {
                    handlePlanProjectedTopology(projectedTopologyId, sourceTopologyInfo, topologyDTOs);
                } else {
                    handleLiveProjectedTopology(projectedTopologyId, sourceTopologyInfo, topologyDTOs);
                }
            });
    }

    private void handlePlanProjectedTopology(long projectedTopologyId,
                                             TopologyInfo sourceTopologyInfo,
                                             RemoteIterator<TopologyDTO.TopologyEntityDTO> dtosIterator) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        logger.info("Receiving projected plan topology, context: {}, projected id: {}, "
                        + "source id: {}, " + "source topology creation time: {}",
                topologyContextId, projectedTopologyId,
                sourceTopologyInfo.getTopologyId(), sourceTopologyInfo.getCreationTime());

        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(topologyContextId,
                projectedTopologyId, sourceTopologyInfo.getCreationTime());
            try {
                int numEntities = planStatsWriter.processProjectedChunks(topologyOrganizer, dtosIterator);
                availabilityTracker.projectedTopologyAvailable(topologyContextId, TopologyContextType.PLAN);

                SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                    .observe(numEntities);
            } catch (CommunicationException | TimeoutException | InterruptedException
                | VmtDbException e) {
                logger.warn("Error occurred while processing data for projected topology "
                        + "broadcast " + topologyOrganizer.getTopologyId(), e);
                throw new RuntimeException("Error occurred while receiving topology broadcast", e);
            }
    }

    private void handleLiveProjectedTopology(long projectedTopologyId,
                                             TopologyInfo sourceTopologyInfo,
                                             RemoteIterator<TopologyDTO.TopologyEntityDTO> dtosIterator) {
        final long topologyContextId = sourceTopologyInfo.getTopologyContextId();
        logger.info("Receiving projected live topology, context: {}, projected id: {}, "
                    + "source id: {} (ignoring)",
                    topologyContextId, projectedTopologyId, sourceTopologyInfo.getTopologyId());
        try {
            final long numEntities = projectedStatsStore.updateProjectedTopology(dtosIterator);

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

    /**
     * Callback receiving the price index. Persist the priceIndex information. After that,
     * the topology snapshot is no longer needed.
     *
     * Since the priceIndex processing requires that the corresponding topology be processed first,
     * we must  accommodate the case when the priceIndex information arrives at the history
     * component first. We use an instance of {@link TopologySnapshotRegistry} to track both the
     * TopologySnapshot and the corresponding PriceIndex information to allow processing in the
     * correct order.
     *
     * @param priceIndex The price index message, containing a topologyId, topologyContextId,
     *                   and a list of (oid, priceIndex, priceIndexProjected) triples.
     */
    @Override
    public void onPriceIndexReceived(@Nonnull final PriceIndexDTOs.PriceIndexMessage priceIndex) {

        final long topologyContextId = priceIndex.getTopologyContextId();
        final long topologyId = priceIndex.getTopologyId();
        logger.info("Received price index payload, topologyContextId {}, topology {}, " +
                        "creationTime {}, " + "entities: {}",
                topologyContextId, topologyId, priceIndex.getSourceTopologyCreationTime(),
                priceIndex.getPayloadCount());

        // process the priceIndex information depending on the type of the topology
        if (topologyContextId == realtimeTopologyContextId) {
            priceIndexWriter.persistPriceIndexInfo(topologyContextId, priceIndex.getTopologyId(),
                    priceIndex.getPayloadList());

            projectedStatsStore.updateProjectedPriceIndex(priceIndex);

            availabilityTracker.priceIndexAvailable(topologyContextId, TopologyContextType.LIVE);
        } else {
            planStatsWriter.persistPlanPriceIndexInfo(priceIndex);
            availabilityTracker.priceIndexAvailable(topologyContextId, TopologyContextType.PLAN);
        }
    }
}