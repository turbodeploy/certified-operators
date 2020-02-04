package com.vmturbo.repository.listener;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoDBException;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.market.component.api.AnalysisSummaryListener;
import com.vmturbo.market.component.api.PlanAnalysisTopologyListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyIDFactory;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;


/**
 * Listens to changes in the topology after running the market.
 */
public class MarketTopologyListener implements
        ProjectedTopologyListener,
        PlanAnalysisTopologyListener,
        AnalysisSummaryListener {

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryNotificationSender notificationSender;
    private final TopologyLifecycleManager topologyManager;
    private final Object topologyInfoLock = new Object();
    private final TopologyIDFactory topologyIDFactory;

    @GuardedBy("topologyInfoLock")
    private long latestKnownProjectedTopologyId = -1;

    public MarketTopologyListener(@Nonnull final RepositoryNotificationSender notificationSender,
                                  @Nonnull final TopologyLifecycleManager topologyManager,
                                  @Nonnull final TopologyIDFactory topologyIDFactory) {
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.topologyManager = Objects.requireNonNull(topologyManager);
        this.topologyIDFactory = topologyIDFactory;
    }

    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                @Nonnull final TopologyInfo originalTopologyInfo,
                @Nonnull final RemoteIterator<ProjectedTopologyEntity> projectedTopo) {
        try {
            onProjectedTopologyReceivedInternal(projectedTopologyId, originalTopologyInfo,
                    projectedTopo);
        } catch (CommunicationException | InterruptedException e) {
            logger.error(
                    "Failed to send notification about received topology " + projectedTopologyId, e);
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
        // should we skip this one? We will skip real time topology if only if the most up to
        // date projected topology received so far, is "newer" than the one we just received.
        // Plan topology are not skipped
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

    private void onProjectedTopologyReceivedInternal(long projectedTopologyId,
            TopologyInfo originalTopologyInfo,
            @Nonnull final RemoteIterator<ProjectedTopologyEntity> projectedTopo)
            throws CommunicationException, InterruptedException {
        if (originalTopologyInfo.getTopologyType() == TopologyType.REALTIME) {
            updateLatestKnownProjectedTopologyId(projectedTopologyId);
        }

        final long topologyContextId = originalTopologyInfo.getTopologyContextId();
        final TopologyID tid = topologyIDFactory.createTopologyID(topologyContextId, projectedTopologyId,
            TopologyID.TopologyType.PROJECTED);

        if (!shouldProcessTopology(originalTopologyInfo, projectedTopologyId)) {
            // skip this real-time topology project cause it looks stale.
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
            notificationSender.onProjectedTopologyAvailable(projectedTopologyId, topologyContextId);
            timer.observe();
        } catch (InterruptedException e) {
            logger.error("Thread interrupted receiving topology " + projectedTopologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onProjectedTopologyFailure(projectedTopologyId, topologyContextId,
                    "Thread interrupted when receiving projected topology " + projectedTopologyId +
                            " + for context " + topologyContextId + ": " + e.getMessage());
            topologyCreator.rollback();
            throw e;
        } catch (CommunicationException | TimeoutException | TopologyEntitiesException
            | GraphDatabaseException | ArangoDBException e) {
            logger.error(
                "Error occurred during retrieving projected topology " + projectedTopologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onProjectedTopologyFailure(projectedTopologyId, topologyContextId,
                "Error receiving projected topology " + projectedTopologyId +
                        " + for context " + topologyContextId + ": " + e.getMessage());
            topologyCreator.rollback();
            return;
        } catch (Exception e) {
            logger.error("Exception while receiving projected topology " + projectedTopologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.PROJECTED_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onProjectedTopologyFailure(projectedTopologyId, topologyContextId,
                    "Error receiving projected topology " + projectedTopologyId +
                            " + for context " + topologyContextId + ": " + e.getMessage());
                topologyCreator.rollback();
            throw e;
        }
    }

    @Override
    public void onPlanAnalysisTopology(final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> topologyDTOs) {
        try {
            onPlanAnalysisTopologyReceivedInternal(topologyInfo, topologyDTOs);
        } catch (CommunicationException | InterruptedException e) {
            logger.error(
                "Failed to send notification about received plan id {} with" +
                    "Topology ID: {} and Topology Type: {}",
                topologyInfo.getTopologyContextId(),
                topologyInfo.getTopologyId(), topologyInfo.getTopologyType(), e);
        } catch (Exception e) {
            logger.error("Failure in processing of plan analysis topology plan ID : " +
                    topologyInfo.getTopologyContextId() + " topology ID : " +
                    topologyInfo.getTopologyId(), e);
        }
    }

    /**
     * A plan analysis topology has been broadcasted -- onPlanAnalysisTopology is your chance to
     * process it.
     *
     * @param topologyInfo TopologyInfo describing the topology
     * @param entityIterator A remote iterator for receiving the plan analysis topology entities.
     * @throws CommunicationException Throws CommunicationException
     * @throws InterruptedException  Throws InterruptedException
     */
    private void onPlanAnalysisTopologyReceivedInternal(TopologyInfo topologyInfo,
                                                @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator)
            throws CommunicationException, InterruptedException {

        try {
            final long topologyId = topologyInfo.getTopologyId();
            final long topologyContextId = topologyInfo.getTopologyContextId();
            logger.info("Received SOURCE topology {} for context {} topology DTOs from Market component",
                    topologyId, topologyContextId);

            final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
                    .labels(SharedMetrics.SOURCE_LABEL)
                    .startTimer();
            final TopologyID tid = topologyIDFactory.createTopologyID(topologyContextId, topologyId, TopologyID.TopologyType.SOURCE);
            final TopologyCreator<TopologyEntityDTO> topologyCreator = topologyManager.newSourceTopologyCreator(tid, topologyInfo);

            TopologyEntitiesUtil.createTopology(entityIterator, topologyId, topologyContextId, timer,
                    tid, topologyCreator, notificationSender);
        } catch (Exception e) {
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onSourceTopologyFailure(topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId(),
                    "Error receiving plan analysis for topology id " + topologyInfo.getTopologyId() +
                            " and plan id " + topologyInfo.getTopologyContextId()  + " : " + e.getMessage());
            throw e;
        }
    }

}
