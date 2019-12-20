package com.vmturbo.repository.listener;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary.ResultCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyIDFactory;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.TopologySummaryListener;

/**
 * Handler for entity information coming in from the Topology Processor. Only used to receive
 * live topology.
 */
public class TopologyEntitiesListener implements EntitiesListener, TopologySummaryListener {

    private final Logger logger = LoggerFactory.getLogger(TopologyEntitiesListener.class);

    private final RepositoryNotificationSender notificationSender;
    private final TopologyLifecycleManager topologyManager;
    private final TopologyIDFactory topologyIDFactory;

    private Object topologyInfoLock = new Object();

    @GuardedBy("topologyInfoLock")
    private TopologyInfo latestKnownRealtimeTopologyInfo = null;

    public TopologyEntitiesListener(@Nonnull final TopologyLifecycleManager topologyManager,
                                    @Nonnull final RepositoryNotificationSender sender,
                                    @Nonnull final TopologyIDFactory topologyIDFactory) {
        this.notificationSender = Objects.requireNonNull(sender);
        this.topologyManager = Objects.requireNonNull(topologyManager);
        this.topologyIDFactory = topologyIDFactory;
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
        final TopologyInfo topologyInfo = topologySummary.getTopologyInfo();
        if (topologyInfo.getTopologyType() == TopologyType.REALTIME &&
                topologySummary.getResultCase() == ResultCase.SUCCESS) {
            synchronized (topologyInfoLock) {
                logger.info("Received topology summary. Setting latest known realtime topology" +
                    " id to {}", topologyInfo.getTopologyId());
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
    public void onTopologyNotification(@Nonnull TopologyInfo topologyInfo,
                                       @Nonnull RemoteIterator<DataSegment> entityIterator) {
        try {
            onTopologyNotificationInternal(topologyInfo, entityIterator);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Error processing realtime topology."
                + " Topology Id: " + topologyInfo.getTopologyId()
                + " Context Id: " + topologyInfo.getTopologyContextId(), e);
        }
    }

    private void onTopologyNotificationInternal(TopologyInfo topologyInfo,
                                                final RemoteIterator<DataSegment> entityIterator)
        throws CommunicationException, InterruptedException {
        final long topologyId = topologyInfo.getTopologyId();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        logger.info("Received topology {} for context {} topology DTOs from TopologyProcessor",
                topologyId, topologyContextId);

        // check if we should skip this one
        if (!shouldProcessTopology(topologyInfo)) {
            logger.info("Skipping processing of topology {}, since it appears to be older than"
                    +" latest known realtime topology.", topologyId);
            TopologyEntitiesUtil.drainIterator(entityIterator, topologyId);
            return;
        }

        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.SOURCE_LABEL)
            .startTimer();
        final TopologyID tid = topologyIDFactory.createTopologyID(topologyContextId, topologyId, TopologyID.TopologyType.SOURCE);
        final TopologyCreator<TopologyEntityDTO> topologyCreator =
            topologyManager.newSourceTopologyCreator(tid, topologyInfo);
        TopologyEntitiesUtil.createTopology(entityIterator, topologyId, topologyContextId, timer,
                tid, topologyCreator, notificationSender);
    }

}
