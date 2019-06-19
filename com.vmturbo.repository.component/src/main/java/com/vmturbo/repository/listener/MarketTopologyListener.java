package com.vmturbo.repository.listener;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoDBException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.AnalysisSummaryListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;

/**
 * Listens to changes in the topology after running the market.
 */
public class MarketTopologyListener implements ProjectedTopologyListener, AnalysisSummaryListener {

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryNotificationSender notificationSender;
    private final TopologyLifecycleManager topologyManager;
    private final Object topologyInfoLock = new Object();

    @GuardedBy("topologyInfoLock")
    private long latestKnownProjectedTopologyId = -1;

    public MarketTopologyListener(@Nonnull final RepositoryNotificationSender notificationSender,
                                  @Nonnull final TopologyLifecycleManager topologyManager) {
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.topologyManager = Objects.requireNonNull(topologyManager);
    }

    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                @Nonnull final TopologyInfo originalTopologyInfo,
                @Nonnull final RemoteIterator<ProjectedTopologyEntity> projectedTopo) {
        try {
            onProjectedTopologyReceivedInternal(projectedTopologyId, originalTopologyInfo,
                    projectedTopo);
        } catch (CommunicationException | InterruptedException e) {
            // TODO This message is very required by plan orchestrator. Need to do something here
            logger.error(
                    "Faled to send notification about received topology " + projectedTopologyId, e);
        }
    }

    private void updateLatestKnownProjectedTopologyId(final long id) {
        synchronized (topologyInfoLock) {
            latestKnownProjectedTopologyId = Math.max(id, latestKnownProjectedTopologyId);
        }
    }

    @Override
    public void onAnalysisSummary(@Nonnull final AnalysisSummary analysisSummary){
            TopologyInfo topologyInfo = analysisSummary.getSourceTopologyInfo();
            if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
                synchronized (topologyInfoLock) {
                    updateLatestKnownProjectedTopologyId(
                        analysisSummary.getProjectedTopologyInfo().getProjectedTopologyId());
                    logger.info("Setting latest known projected realtime topology id to {}, " +
                            "referring to source topology {}",
                        latestKnownProjectedTopologyId, topologyInfo.getTopologyId());
                }
            }
    }

    private boolean shouldProcessTopology(TopologyInfo originalTopologyInfo, long projectedTopologyId) {
        // should we skip this one? We will skip it only if the most up to date projected
        // topology received so far, is "newer" than the one we just received.
        if (originalTopologyInfo.getTopologyType() == TopologyType.REALTIME) {
            // don't process if this projected topology is older than the once we already
            // received.
            synchronized (topologyInfoLock) {
                if (latestKnownProjectedTopologyId > projectedTopologyId) {
                    return false;
                }
            }
        }
        return true;
    }

    public void onProjectedTopologyReceivedInternal(long projectedTopologyId,
            TopologyInfo originalTopologyInfo,
            @Nonnull final RemoteIterator<ProjectedTopologyEntity> projectedTopo)
            throws CommunicationException, InterruptedException {

        updateLatestKnownProjectedTopologyId(projectedTopologyId);

        final long topologyContextId = originalTopologyInfo.getTopologyContextId();
        final TopologyID tid = new TopologyID(topologyContextId, projectedTopologyId,
            TopologyID.TopologyType.PROJECTED);

        if (!shouldProcessTopology(originalTopologyInfo, projectedTopologyId)) {
            // skip this realtime topology project cause it looks stale.
            logger.info("Skipping stale realtime projected topology id {} for source topology id {}",
                    projectedTopologyId, originalTopologyInfo.getTopologyId());
            // drain the iterator and exit.
            try {
                while (projectedTopo.hasNext()) {
                    projectedTopo.nextChunk();
                }
            } catch (TimeoutException e) {
                logger.warn("TimeoutException while skipping projected topology {}", projectedTopologyId);
            } finally {
                SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.SKIPPED_LABEL).increment();
            }
            return;
        }

        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
                .labels(SharedMetrics.PROJECTED_LABEL)
                .startTimer();

        final TopologyCreator<ProjectedTopologyEntity> topologyCreator =
            topologyManager.newProjectedTopologyCreator(tid, originalTopologyInfo);
        try {
            topologyCreator.initialize();
            logger.info("Start updating topology {}", tid);
            int numberOfEntities = 0;
            int chunkNumber = 0;
            while (projectedTopo.hasNext()) {
                Collection<ProjectedTopologyEntity> chunk = projectedTopo.nextChunk();
                logger.debug("Received chunk #{} of size {} for topology {}", ++chunkNumber, chunk.size(), tid);
                topologyCreator.addEntities(chunk);
                numberOfEntities += chunk.size();
            }
            SharedMetrics.TOPOLOGY_ENTITY_COUNT_GAUGE
                .labels(SharedMetrics.PROJECTED_LABEL)
                .setData((double) numberOfEntities);
            topologyCreator.complete();
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.PROCESSED_LABEL).increment();
            logger.info("Finished updating topology {} with {} entities", tid, numberOfEntities);
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + projectedTopologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.FAILED_LABEL).increment();
            topologyCreator.rollback();
            return;
        } catch (CommunicationException | TimeoutException | TopologyEntitiesException
            | GraphDatabaseException | ArangoDBException e) {
            logger.error(
                "Error occurred during retrieving projected topology " + projectedTopologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onProjectedTopologyFailure(projectedTopologyId, topologyContextId,
                "Error receiving projected topology " + projectedTopologyId
                    + ": " + e.getMessage());
            topologyCreator.rollback();
            return;
        } catch (RuntimeException e) {
            logger.error("Exception while receiving projected topology " + projectedTopologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.FAILED_LABEL).increment();
            topologyCreator.rollback();
            throw e;
        }
        notificationSender.onProjectedTopologyAvailable(projectedTopologyId, topologyContextId);
        timer.observe();
    }
}
