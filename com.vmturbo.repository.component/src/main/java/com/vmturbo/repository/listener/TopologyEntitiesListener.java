package com.vmturbo.repository.listener;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.IncrementalTopologyRelationshipRecorder;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.SourceTopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.TopologySummaryListener;

/**
 * Handler for entity information coming in from the Topology Processor. Only used to receive
 * live topology.
 */
public class TopologyEntitiesListener implements EntitiesListener, TopologySummaryListener {
    private final Logger logger = LoggerFactory.getLogger(TopologyEntitiesListener.class);

    private final TopologyRelationshipRecorder globalSupplyChainRecorder;

    private final RepositoryNotificationSender notificationSender;
    private final TopologyLifecycleManager topologyManager;

    private Object topologyInfoLock = new Object();

    @GuardedBy("topologyInfoLock")
    private TopologyInfo latestKnownRealtimeTopologyInfo = null;

    public TopologyEntitiesListener(@Nonnull final TopologyLifecycleManager topologyManager,
                                    @Nonnull final TopologyRelationshipRecorder globalSupplyChainRecorder,
                                    @Nonnull final RepositoryNotificationSender sender) {
        this.globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorder);
        this.notificationSender = Objects.requireNonNull(sender);
        this.topologyManager = Objects.requireNonNull(topologyManager);
    }


    /**
     * Consume a {@link TopologySummary} message. This method is synchronized to guard access to the
     * "last known topology info" object.
     *
     * @param topologySummary
     */
    @Override
    public void onTopologySummary(final TopologySummary topologySummary) {
        // the topology summaries will generally arrive before the rest of the topology data. We
        // will keep track of the most recently received one and use it to try to detect situations
        // where we may be processing stale realtime topologies.
        TopologyInfo topologyInfo = topologySummary.getTopologyInfo();
        if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
            synchronized (topologyInfoLock) {
                logger.debug("Setting latest known realtime topology id to {}", topologyInfo.getTopologyId());
                latestKnownRealtimeTopologyInfo = topologySummary.getTopologyInfo();
            }
        }
    }

    /**
     * Should this topology be processed? We will not process the topology if it is deemed to be
     * "stale". A topology is considered "stale" if there is a newer version of it available in kafka,
     * as reported by the {@link TopologySummary} messages.
     *
     * Only realtime topologies would be checked for staleness -- plan topologies will always be
     * recommended for processing.
     *
     * This method is synchronized to guard the access to the "last known topology info" object.
     *
     * @param incomingTopologyInfo the {@link TopologyInfo} for the topology to be considered for
     *                             processing.
     * @return true, if the topology should be processed.
     */
    private boolean shouldProcessTopology(@Nonnull final TopologyInfo incomingTopologyInfo) {
        // always process non-realtime topologies
        if (incomingTopologyInfo.getTopologyType() != TopologyType.REALTIME) {
            return true;
        }

        synchronized (topologyInfoLock) {
            if (latestKnownRealtimeTopologyInfo == null) {
                return true; // might as well process it.
            }

            // for realtime topologies, only process if the topology id is the same or greater than the
            // last known topology id. NOTE: we can check the creation time instead, but we expect the
            // topology id's to be generated in increasing order, so we shouldn't need to.
            return (incomingTopologyInfo.getCreationTime() >= latestKnownRealtimeTopologyInfo.getCreationTime());
        }
    }

    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {
        try {
            onTopologyNotificationInternal(topologyInfo, entityIterator);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Error performing topology notifications", e);
        }
    }

    private void onTopologyNotificationInternal(TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator)
            throws CommunicationException, InterruptedException {

        final long topologyId = topologyInfo.getTopologyId();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        logger.info("Received topology {} for context {} topology DTOs from TopologyProcessor",
                topologyId, topologyContextId);

        // check if we should skip this one
        if (!shouldProcessTopology(topologyInfo)) {
            logger.info("Skipping processing of topology {}, since it appears to be older than"
                    +" latest known realtime topology.", topologyId);
            // drain the iterator and exit.
            try {
                while (entityIterator.hasNext()) {
                    entityIterator.nextChunk();
                }
            } catch (TimeoutException e) {
                logger.warn("TimeoutException while skipping topology {}", topologyId);
            } finally {
                SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.SKIPPED_LABEL).increment();
            }
            return;
        }


        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.SOURCE_LABEL)
            .startTimer();
        final TopologyID tid = new TopologyID(topologyContextId, topologyId, TopologyID.TopologyType.SOURCE);
        SourceTopologyCreator topologyCreator = topologyManager.newSourceTopologyCreator(tid);
        try {
            logger.info("Start updating topology {}", tid);
            topologyCreator.initialize();
            IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
            int numberOfEntities = 0;
            int chunkNumber = 0;
            while (entityIterator.hasNext()) {
                Collection<TopologyEntityDTO> chunk = entityIterator.nextChunk();
                logger.debug("Received chunk #{} of size {} for topology {}", ++chunkNumber, chunk.size(), tid);
                recorder.processChunk(chunk);
                topologyCreator.addEntities(chunk);
                numberOfEntities += chunk.size();
            }
            globalSupplyChainRecorder.setGlobalSupplyChainProviderRels(recorder.supplyChain());
            topologyCreator.complete();
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_GAUGE
                .labels(SharedMetrics.SOURCE_LABEL)
                .setData((double)numberOfEntities);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.PROCESSED_LABEL).increment();
            logger.info("Finished updating topology {} with {} entities", tid, numberOfEntities);
            notificationSender.onSourceTopologyAvailable(topologyId, topologyContextId);
        } catch (GraphDatabaseException | CommunicationException |
                TopologyEntitiesException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId, e);
            topologyCreator.rollback();
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId, e);
            topologyCreator.rollback();
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            logger.error("Exception while receiving topology " + topologyId, e);
            topologyCreator.rollback();
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
            throw e;
        }

        timer.observe();
    }
}
