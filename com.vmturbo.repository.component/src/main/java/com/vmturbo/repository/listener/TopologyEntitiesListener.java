package com.vmturbo.repository.listener;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.IncrementalTopologyRelationshipRecorder;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor. Only used to receive
 * live topology.
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LoggerFactory.getLogger(TopologyEntitiesListener.class);

    private final TopologyRelationshipRecorder globalSupplyChainRecorder;

    private final RepositoryNotificationSender notificationSender;
    private final TopologyLifecycleManager topologyManager;

    public TopologyEntitiesListener(@Nonnull final TopologyLifecycleManager topologyManager,
                                    @Nonnull final TopologyRelationshipRecorder globalSupplyChainRecorder,
                                    @Nonnull final RepositoryNotificationSender sender) {
        this.globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorder);
        this.notificationSender = Objects.requireNonNull(sender);
        this.topologyManager = Objects.requireNonNull(topologyManager);
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

        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.SOURCE_LABEL)
            .startTimer();
        final TopologyID tid = new TopologyID(topologyContextId, topologyId, TopologyID.TopologyType.SOURCE);
        TopologyCreator topologyCreator = topologyManager.newTopologyCreator(tid);
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
            logger.info("Finished updating topology {} with {} entities", tid, numberOfEntities);
            notificationSender.onSourceTopologyAvailable(topologyId, topologyContextId);
        } catch (GraphDatabaseException | CommunicationException |
                TopologyEntitiesException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId, e);
            topologyCreator.rollback();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId, e);
            topologyCreator.rollback();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            logger.error("Exception while receiving topology " + topologyId, e);
            topologyCreator.rollback();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
            throw e;
        }

        timer.observe();
    }
}
