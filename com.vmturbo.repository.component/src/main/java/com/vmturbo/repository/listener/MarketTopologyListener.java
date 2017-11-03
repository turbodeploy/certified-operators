package com.vmturbo.repository.listener;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoDBException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.operator.TopologyGraphCreator;
import com.vmturbo.repository.topology.TopologyConverter;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufWriter;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Listens to changes in the topology after running the market.
 */
public class MarketTopologyListener implements ProjectedTopologyListener {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyEventHandler topologyEventHandler;
    private final RepositoryNotificationSender notificationSender;
    private final TopologyProtobufsManager topologyProtobufsManager;

    public MarketTopologyListener(@Nonnull final TopologyEventHandler topologyEventHandler,
                                  @Nonnull final RepositoryNotificationSender notificationSender,
                                  @Nonnull final TopologyProtobufsManager rawTopologiesManager) {
        this.topologyEventHandler = Objects.requireNonNull(topologyEventHandler);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.topologyProtobufsManager = Objects.requireNonNull(rawTopologiesManager);
    }

    @Override
    public void onProjectedTopologyReceived(long projectedTopologyId, TopologyInfo originalTopologyInfo,
            @Nonnull final RemoteIterator<TopologyEntityDTO> projectedTopo) {
        try {
            onProjectedTopologyReceivedInternal(projectedTopologyId, originalTopologyInfo,
                    projectedTopo);
        } catch (CommunicationException | InterruptedException e) {
            // TODO This message is very required by plan orchestrator. Need to do something here
            logger.error(
                    "Faled to send notification about received topology " + projectedTopologyId, e);
        }
    }

    public void onProjectedTopologyReceivedInternal(long projectedTopologyId,
            TopologyInfo originalTopologyInfo,
            @Nonnull final RemoteIterator<TopologyEntityDTO> projectedTopo)
            throws CommunicationException, InterruptedException {
        final long topologyContextId = originalTopologyInfo.getTopologyContextId();
        final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId,
            TopologyType.PROJECTED);
        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
                .labels(SharedMetrics.PROJECTED_LABEL)
                .startTimer();
        TopologyGraphCreator graphCreator = null;
        TopologyProtobufWriter topoWriter = null;

        try {
            logger.info("Start updating topology {}",  tid);
            graphCreator = topologyEventHandler.initializeTopologyGraph(tid);
            topoWriter = topologyProtobufsManager.createTopologyProtobufWriter(tid.getTopologyId());
            int numberOfEntities = 0;
            int chunkNumber = 0;
            while (projectedTopo.hasNext()) {
                Collection<TopologyEntityDTO> chunk = projectedTopo.nextChunk();
                logger.debug("Received chunk #{} of size {} for topology {}", ++chunkNumber, chunk.size(), tid);
                final Collection<ServiceEntityRepoDTO> repoDTOs = TopologyConverter.convert(chunk);
                graphCreator.updateTopologyToDb(repoDTOs);
                topoWriter.storeChunk(chunk);
                numberOfEntities += chunk.size();
            }
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_GAUGE
                .labels(SharedMetrics.PROJECTED_LABEL)
                .setData((double)numberOfEntities);
            topologyEventHandler.register(tid);
            logger.info("Finished updating topology {} with {} entities", tid, numberOfEntities);
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + projectedTopologyId, e);
            rollback(graphCreator, topoWriter, tid);
            return;
        } catch (CommunicationException | TimeoutException
                        | GraphDatabaseException | ArangoDBException e) {
            logger.error(
                "Error occurred during retrieving projected topology " + projectedTopologyId, e);
            notificationSender.onProjectedTopologyFailure(projectedTopologyId, topologyContextId,
                "Error receiving projected topology " + projectedTopologyId
                    + ": " + e.getMessage());
            rollback(graphCreator, topoWriter, tid);
            return;
        } catch (RuntimeException e) {
            logger.error("Exception while receiving projected topology " + projectedTopologyId, e);
            rollback(graphCreator, topoWriter, tid);
            throw e;
        }
        notificationSender.onProjectedTopologyAvailable(projectedTopologyId, topologyContextId);
        timer.observe();
    }

    private void rollback(@Nullable TopologyGraphCreator graphCreator,
                    @Nullable TopologyProtobufWriter writer, TopologyID tid) {
        if (graphCreator != null) {
            topologyEventHandler.dropDatabase(tid);
        }
        if (writer != null) {
            writer.delete();
        }
    }
}
