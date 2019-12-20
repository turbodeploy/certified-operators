package com.vmturbo.history.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.stats.StatsWriteCoordinator;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Listen for updates to the topology;  record them to the DB.
 **/
public class LiveTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private static final int MAX_CONCURRENT_LIVE_TOPOLOGIES = 1;

    // the database access utility classes for the History-related RDB tables
    private final StatsWriteCoordinator statsWriteCoordinator;
    private final StatsAvailabilityTracker availabilityTracker;

    // keeps track of the live topologies being processed. Used to prevent processing more than the
    // expected amount.
    private final List<TopologyInfo> topologiesInProcess = new ArrayList<>();

    public LiveTopologyEntitiesListener(@Nonnull final StatsWriteCoordinator statsWriteCoordinator,
                    @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker) {
        this.statsWriteCoordinator = Objects.requireNonNull(statsWriteCoordinator);
        this.availabilityTracker = Objects.requireNonNull(statsAvailabilityTracker);
    }

    /**
     * Receive a new Topology and call the {@link HistorydbIO} to persist the elements of the topology.
     * The Topology is received in chunks, using {@link RemoteIterator}, to reduce maximum memory footprint required.
     * An instance of TopologySnapshotRegistry is used to coordinate the async receipt of the Topology and
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
                                       RemoteIterator<DataSegment> topologyDTOs) {
        try (final DataMetricTimer timer = SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY.labels(
                SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL, SharedMetrics.
                        LIVE_CONTEXT_TYPE_LABEL).startTimer()) {
            handleLiveTopology(topologyInfo, topologyDTOs);
        } catch (Exception e) {
            logger.error("An error happened while persisting the plan topology.", e);
        }
    }

    /**
     * Skip processing of the rest of the topology in the iterator by consuming the chunks and
     * doing nothing with them.
     *
     * @param topologyInfo The topology information.
     * @param iterator     The data segment iterator.
     */
    private void skipRestOfTopology(TopologyInfo topologyInfo,
                                    RemoteIterator<DataSegment> iterator) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        logger.warn("Going to skip writing data for topology {} since one is still in progress.",
                topologyId);
        // drain the remote iterator
        try {
            while (iterator.hasNext()) {
                iterator.nextChunk();
            }
        } catch (InterruptedException | CommunicationException | TimeoutException e) {
            logger.warn("Ignoring error occurred while skipping realtime topology broadcast " + topologyId, e);
        }
    }

    private void handleLiveTopology(TopologyInfo topologyInfo,
                                    RemoteIterator<DataSegment> dtosIterator)
            throws CommunicationException, InterruptedException {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        final long creationTime = topologyInfo.getCreationTime();
        logger.info("Receiving live topology, context: {}, id: {}", topologyContextId, topologyId);
        synchronized (topologiesInProcess) {
            if (topologiesInProcess.size() >= MAX_CONCURRENT_LIVE_TOPOLOGIES) {
                // skip this live topology if another is still in progress
                skipRestOfTopology(topologyInfo, dtosIterator);
                return;
            }
            // not skipping -- add to list of topologies being processed
            topologiesInProcess.add(topologyInfo);
        }
        try {
            int numEntities = statsWriteCoordinator.processChunks(topologyInfo, dtosIterator);
            availabilityTracker.topologyAvailable(topologyContextId, TopologyContextType.LIVE,
                    true);

            SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL, SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                .observe((double)numEntities);
        } catch (Exception e) {
            logger.warn("Error occurred while processing data for realtime topology broadcast "
                    + topologyInfo.getTopologyId(), e);
            availabilityTracker.topologyAvailable(topologyContextId, TopologyContextType.LIVE,
                    false);
        } finally {
            // remove from the in-progress list
            synchronized (topologiesInProcess) {
                topologiesInProcess.remove(topologyInfo);
            }
        }
    }
}
