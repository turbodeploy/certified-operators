package com.vmturbo.history.topology;

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
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.market.component.api.PlanAnalysisTopologyListener;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Listen for updates to the plan topology; record them to the DB.
 **/
public class PlanTopologyEntitiesListener implements PlanAnalysisTopologyListener {

    private final Logger logger = LogManager.getLogger();

    // the database access utility classes for the History-related RDB tables
    private final PlanStatsWriter planStatsWriter;

    private final StatsAvailabilityTracker availabilityTracker;

    public PlanTopologyEntitiesListener(
            @Nonnull final PlanStatsWriter planStatsWriter,
            @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker) {
        this.planStatsWriter = Objects.requireNonNull(planStatsWriter);
        this.availabilityTracker = Objects.requireNonNull(statsAvailabilityTracker);
    }

    /**
     * Receive a new Plan Analysis Topology and call the {@link HistorydbIO} to persist the elements
     * of the topology. The Topology is received in chunks, using {@link RemoteIterator}, to reduce
     * maximum memory footprint required. An instance of {@link TopologySnapshotRegistry} is used to
     * coordinate the async receipt of the plan analysis Topology and PriceIndex information.
     *
     * The entire collection of ServiceEntities is required for two reasons:
     * <ol>
     *     <li>the utilization calculation for commodities bought requires the seller entity to look
     *     up the capacity
     *     <li>persisting the price index requires the EntityType of the SE; we should save just the
     *     types to satisfy this one, but (1) requires the commodities too
     * </ol>
     *
     */
    @Override
    public void onPlanAnalysisTopology(final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> topologyDTOs) {
        try (DataMetricTimer timer = SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY.labels(
                SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                .startTimer()) {
            handleScopedTopology(topologyInfo, topologyDTOs);
        } catch (Exception e) {
            logger.error("And error happened while persisting the plan topology.", e);
        }
    }

    private void handleScopedTopology(TopologyInfo topologyInfo,
            RemoteIterator<TopologyDTO.Topology.DataSegment> dtosIterator) throws CommunicationException, InterruptedException {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        logger.info("Receiving plan topology, context: {}, id: {}", topologyContextId, topologyId);
        try {
            int numEntities = planStatsWriter.processChunks(topologyInfo, dtosIterator);
            availabilityTracker.topologyAvailable(topologyContextId, TopologyContextType.PLAN,
                    true);

            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                    .observe((double)numEntities);
        } catch (Exception e) {
            logger.warn("Error occurred while processing data for topology broadcast "
                    + topologyInfo.getTopologyId(), e);
            availabilityTracker.topologyAvailable(topologyContextId, TopologyContextType.PLAN,
                    false);
        }
    }
}
