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
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.IncrementalTopologyRelationshipRecorder;
import com.vmturbo.repository.topology.TopologyConverter;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor.
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LoggerFactory.getLogger(TopologyEntitiesListener.class);

    private final TopologyEventHandler topologyEventHandler;
    private final TopologyRelationshipRecorder globalSupplyChainRecorder;

    private final long realtimeTopologyContextId;
    private final RepositoryNotificationSender notificationSender;

    public TopologyEntitiesListener(final TopologyEventHandler topologyEventHandler,
                                    final TopologyRelationshipRecorder globalSupplyChainRecorder,
                                    final long realtimeTopologyContextId,
                                    @Nonnull final RepositoryNotificationSender sender) {
        this.topologyEventHandler = Objects.requireNonNull(topologyEventHandler);
        this.globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorder);

        this.notificationSender = sender;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void onTopologyNotification(TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<TopologyEntityDTO> entityIterator) {

        final long topologyId = topologyInfo.getTopologyId();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        logger.info("Received topology {} for context {} topology DTOs from TopologyProcessor",
                topologyId, topologyContextId);

        if (realtimeTopologyContextId != topologyContextId) {
            logger.info("Skipped persisting the non-realtime topology {} for context {}",
                topologyId, topologyContextId);
            ignoreData(entityIterator);
            return;
        }

        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.SOURCE_LABEL)
            .startTimer();
        final TopologyID tid = new TopologyID(topologyContextId, topologyId, TopologyType.SOURCE);
        TopologyGraphCreator graphCreator = null;
        try {
            logger.info("Start updating topology {}", tid);
            graphCreator = topologyEventHandler.initializeTopologyGraph(tid);
            IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
            int numberOfEntities = 0;
            int chunkNumber = 0;
            while (entityIterator.hasNext()) {
                Collection<TopologyEntityDTO> chunk = entityIterator.nextChunk();
                logger.debug("Received chunk #{} of size {} for topology {}", ++chunkNumber, chunk.size(), tid);
                recorder.processChunk(chunk);
                final Collection<ServiceEntityRepoDTO> repoDTOs = TopologyConverter.convert(chunk);
                graphCreator.updateTopologyToDb(repoDTOs);
                numberOfEntities += chunk.size();
            }
            globalSupplyChainRecorder.setGlobalSupplyChainProviderRels(recorder.supplyChain());
            topologyEventHandler.register(tid);
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_GAUGE
                .labels(SharedMetrics.SOURCE_LABEL)
                .setData((double)numberOfEntities);
            logger.info("Finished updating topology {} with {} entities", tid, numberOfEntities);
            notificationSender.onSourceTopologyAvailable(topologyId, topologyContextId);
        } catch (GraphDatabaseException | CommunicationException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId, e);
            rollback(graphCreator, tid);
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId, e);
            rollback(graphCreator, tid);
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
        } catch (Exception e) {
            logger.error("Exception while receiving topology " + topologyId, e);
            rollback(graphCreator, tid);
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId + ": " + e.getMessage());
            throw e;
        }

        timer.observe();
    }

    // TODO (pbaranchikov) instead add method to close remote iterator
    private void ignoreData(@Nonnull RemoteIterator<?> remoteIterator) {
        try {
            while (remoteIterator.hasNext()) {
                remoteIterator.nextChunk();
            }
        } catch (InterruptedException | TimeoutException | CommunicationException e) {
            logger.warn("Error experienced, while awaiting for the next chunk (which is really " +
                    "ignored)", e);
        }
    }

    private void rollback(TopologyGraphCreator graphCreator, TopologyID tid) {
        if (graphCreator != null) {
            topologyEventHandler.dropDatabase(tid);
        }
    }
}
