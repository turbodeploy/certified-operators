package com.vmturbo.history.topology;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for updates to the plan topology; record them to the DB.
 **/
public class PlanTopologyEntitiesListener implements EntitiesListener {

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
     * Receive a new Topology and call the {@link HistorydbIO} to persist the elements of the topology.
     * The Topology is received in chunks, using {@link RemoteIterator}, to reduce maximum memory footprint required.
     * An instance of {@link TopologySnapshotRegistry} is used to coordinate the async receipt of the Topology and
     * PriceIndex information.
     * The entire collection of ServiceEntities is required for two reasons:
     * <ol>
     *     <li>the utilization calculation for commodities bought requires the seller entity to look up the capacity
     *     <li>persisting the price index requires the EntityType of the SE; we should save just the types
     *     to satisfy this one, but (1) requires the commodities too
     * </ol>
     *
     */
    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
            RemoteIterator<TopologyEntityDTO> topologyDTOs) {
        SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY.labels(
                SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                .time(() -> {
                    handlePlanTopology(topologyInfo, topologyDTOs);
                });
    }

    private void handlePlanTopology(TopologyInfo topologyInfo,
            RemoteIterator<TopologyEntityDTO> dtosIterator) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        final long creationTime = topologyInfo.getCreationTime();
        logger.info("Receiving plan topology, context: {}, id: {}", topologyContextId, topologyId);
        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(topologyContextId, topologyId, creationTime);
        try {
            int numEntities = planStatsWriter.processChunks(topologyOrganizer, dtosIterator);
            availabilityTracker.topologyAvailable(topologyContextId, TopologyContextType.PLAN);

            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL, SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                    .observe(numEntities);
        } catch (CommunicationException | TimeoutException | InterruptedException
                | VmtDbException e) {
            logger.warn("Error occurred while processing data for topology broadcast "
                    + topologyOrganizer.getTopologyId(), e);
        }
    }
}
